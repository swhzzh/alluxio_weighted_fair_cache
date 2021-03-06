/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.ClientContext;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.meta.UserStorageInfo;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link DefaultBlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultBlockWorker extends AbstractWorker implements BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockWorker.class);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleaner;

  /** Client for all block master communication. */
  private final BlockMasterClient mBlockMasterClient;
  /**
   * Block master clients. commitBlock is the only reason to keep a pool of block master clients
   * on each worker. We should either improve our RPC model in the master or get rid of the
   * necessity to call commitBlock in the workers.
   */
  private final BlockMasterClientPool mBlockMasterClientPool;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;
  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;
  /** Checker for storage paths. **/
  private StorageChecker mStorageChecker;
  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions;
  /** Block Store manager. */
  private BlockStore mBlockStore;
  private WorkerNetAddress mAddress;

  /** The under file system block store. */
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;

  /**
   * The worker ID for this worker. This is initialized in {@link #start(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  private AtomicReference<Long> mWorkerId;

  private final UfsManager mUfsManager;
  private SpaceReserver mSpaceReserver;
  private BlockSpaceUpdater mBlockSpaceUpdater;
  private UserSpaceReporter mUserSpaceReporter;
  //private UserWeightsSync mUserWeightsSync;
  /**
   * Constructs a default block worker.
   *
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(UfsManager ufsManager) {
    this(new BlockMasterClientPool(),
        new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build()),
        new Sessions(), new TieredBlockStore(), ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, BlockStore blockStore,
      UfsManager ufsManager) {
    super(Executors
        .newFixedThreadPool(7, ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mBlockMasterClientPool = blockMasterClientPool;
    mBlockMasterClient = mBlockMasterClientPool.acquire();
    mFileSystemMasterClient = fileSystemMasterClient;
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mMetricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mBlockStore = blockStore;
    mWorkerId = new AtomicReference<>(-1L);

    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
    mUfsManager = ufsManager;
    mUnderFileSystemBlockStore = new UnderFileSystemBlockStore(mBlockStore, ufsManager);

    Metrics.registerGauges(this);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   */
  @Override
  public void start(WorkerNetAddress address) throws IOException {
    mAddress = address;
    try {
      RetryUtils.retry("create worker id", () -> mWorkerId.set(mBlockMasterClient.getId(address)),
          RetryUtils.defaultWorkerMasterClientRetry(ServerConfiguration
              .getDuration(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a worker id from block master: "
          + e.getMessage());
    }

    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    mBlockMasterSync = new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(this, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleaner = new SessionCleaner(mSessions, mBlockStore, mUnderFileSystemBlockStore);

    // Setup space reserver
    mSpaceReserver = new SpaceReserver(this);
    mBlockSpaceUpdater = new BlockSpaceUpdater(this);
    mUserSpaceReporter = new UserSpaceReporter(this);
//    mUserWeightsSync = new UserWeightsSync(this, mFileSystemMasterClient);
//
//
//    getExecutorService().submit(
//        new HeartbeatThread(HeartbeatContext.WORKER_USER_WEIGHTS_SYNC, mUserWeightsSync,
//            (int) FormatUtils.parseTimeSize("1s"),
//            ServerConfiguration.global(), ServerUserState.global()));
    getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_USER_SPACE_REPORTER, mUserSpaceReporter,
            (int) FormatUtils.parseTimeSize("5s"),
            ServerConfiguration.global(), ServerUserState.global()));
    getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SPACE_UPDATER, mBlockSpaceUpdater,
            (int) FormatUtils.parseTimeSize("4s"),
            ServerConfiguration.global(), ServerUserState.global()));

    getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER, mSpaceReserver,
            (int) ServerConfiguration.getMs(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS),
            ServerConfiguration.global(), ServerUserState.global()));

    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            ServerConfiguration.global(), ServerUserState.global()));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            ServerConfiguration.global(), ServerUserState.global()));

    // Setup storage checker
    if (ServerConfiguration.getBoolean(PropertyKey.WORKER_STORAGE_CHECKER_ENABLED)) {
      mStorageChecker = new StorageChecker();
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_STORAGE_HEALTH, mStorageChecker,
              (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
                  ServerConfiguration.global(), ServerUserState.global()));
    }

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleaner);
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   */
  @Override
  public void stop() {
    // Steps to shutdown:
    // 1. Gracefully shut down the runnables running in the executors.
    // 2. Shutdown the executors.
    // 3. Shutdown the clients. This needs to happen after the executors is shutdown because
    //    runnables running in the executors might be using the clients.
    if (mSessionCleaner != null) {
      mSessionCleaner.stop();
    }
    // The executor shutdown needs to be done in a loop with retry because the interrupt
    // signal can sometimes be ignored.
    try {
      CommonUtils.waitFor("block worker executor shutdown", () -> {
        getExecutorService().shutdownNow();
        try {
          return getExecutorService().awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    mBlockMasterClientPool.release(mBlockMasterClient);
    try {
      mBlockMasterClientPool.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the block master client pool with error {}.", e.getMessage());
    }
    mFileSystemMasterClient.close();
  }

  @Override
  public Map<Long, BlockAccessRecord> getBlockAccessRecords() {
    return mBlockStore.getBlockAccessRecords();
  }

  @Override
  public void updateBlockSpace(long userId, long blockId, long spaceUsed) {
    mBlockStore.updateBlockSpace(userId, blockId, spaceUsed);
  }

  @Override
  public void updateUserWeights(Map<Long, Double> weights) {
    mBlockStore.updateUserWeights(weights);
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mBlockStore.abortBlock(sessionId, blockId);
  }

  @Override
  public void abortUserBlock(long sessionId, long userId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException {
    mBlockStore.abortUserBlock(sessionId, userId, blockId);
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mBlockStore.accessBlock(sessionId, blockId);
  }

  @Override
  public void accessUserBlock(long sessionId, long userId, long blockId)
      throws BlockDoesNotExistException {
    mBlockStore.accessUserBlock(sessionId, userId, blockId);
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    // NOTE: this may be invoked multiple times due to retry on client side.
    // TODO(binfan): find a better way to handle retry logic
    try {
      mBlockStore.commitBlock(sessionId, blockId, pinOnCreate);
    } catch (BlockAlreadyExistsException e) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure, therefore ignore the exception", blockId, e);
    }

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(sessionId, blockId);
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      String mediumType = loc.mediumType();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      blockMasterClient.commitBlock(mWorkerId.get(), bytesUsedOnTier, loc.tierAlias(), mediumType,
          blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      mBlockStore.unlockBlock(lockId);
    }
  }

  @Override
  public void commitUserBlock(long sessionId, long userId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    try {
      mBlockStore.commitUserBlock(sessionId, userId, blockId, pinOnCreate);
    } catch (BlockAlreadyExistsException e) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure, therefore ignore the exception", blockId, e);
    }

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockUserBlock(sessionId, userId, blockId);
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      String mediumType = loc.mediumType();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      blockMasterClient.commitBlock(mWorkerId.get(), bytesUsedOnTier, loc.tierAlias(), mediumType,
          blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      mBlockStore.unlockUserBlock(lockId);
    }
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) throws IOException {
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      blockMasterClient.commitBlockInUfs(blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, String tierAlias,
      String medium, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    if (medium.isEmpty()) {
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInTierWithMedium(medium);
    }
    TempBlockMeta createdBlock;
    try {
      createdBlock = mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
    } catch (WorkerOutOfSpaceException e) {
      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new WorkerOutOfSpaceException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e);
    }
    return createdBlock.getPath();
  }

  @Override
  public String createUserBlock(long sessionId, long userId, long blockId, String tierAlias,
      String medium, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    if (medium.isEmpty()){
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInTierWithMedium(medium);
    }
    TempBlockMeta createdBlock;
    try {
      createdBlock = mBlockStore.createUserBlock(sessionId, userId, blockId, loc, initialBytes);
    }catch (WorkerOutOfSpaceException e) {
      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new WorkerOutOfSpaceException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e);
    }
    return createdBlock.getPath();
  }

  @Override
  public void createBlockRemote(long sessionId, long blockId, String tierAlias,
      String medium, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    if (medium.isEmpty()) {
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInTierWithMedium(medium);
    }
    mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
  }

  @Override
  public void createUserBlockRemote(long sessionId, long userId, long blockId, String tierAlias,
      String medium, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    if (medium.isEmpty()) {
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInTierWithMedium(medium);
    }
    mBlockStore.createUserBlock(sessionId, userId, blockId, loc, initialBytes);
  }

  @Override
  public void freeSpace(long sessionId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.freeSpace(sessionId, availableBytes, location);
  }

  @Override
  public void freeUserSpace(long sessionId, long userId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.freeUserSpace(sessionId, userId, availableBytes, location);
  }

  @Override
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    return mBlockStore.getBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return mBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return mBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  @Override
  public BlockMeta getUserVolatileBlockMeta(long userId, long blockId)
      throws BlockDoesNotExistException {
    return mBlockStore.getUserVolatileBlockMeta(userId, blockId);
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return mBlockStore.getBlockMeta(sessionId, blockId, lockId);
  }

  @Override
  public BlockMeta getUserBlockMeta(long sessionId, long userId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return mBlockStore.getUserBlockMeta(sessionId, userId, blockId, lockId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  @Override
  public boolean hasUserBlockMeta(long userId, long blockId) {
    return mBlockStore.hasUserBlockMeta(userId, blockId);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    return mBlockStore.lockBlock(sessionId, blockId);
  }

  @Override
  public long lockUserBlock(long sessionId, long userId, long blockId)
      throws BlockDoesNotExistException {
    return mBlockStore.lockUserBlock(sessionId, userId, blockId);
  }

  @Override
  public long lockBlockNoException(long sessionId, long blockId) {
    return mBlockStore.lockBlockNoException(sessionId, blockId);
  }

  @Override
  public long lockUserBlockNoException(long sessionId, long userId, long blockId) {
    return mBlockStore.lockUserBlockNoException(sessionId, userId, blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // TODO(calvin): Move this logic into BlockStore#moveBlockInternal if possible
    // Because the move operation is expensive, we first check if the operation is necessary
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
    long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mBlockStore.moveBlock(sessionId, blockId, dst);
  }

  @Override
  public void moveBlockToMedium(long sessionId, long blockId, String mediumType)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTierWithMedium(mediumType);
    long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mBlockStore.moveBlock(sessionId, blockId, dst);
  }

  @Override
  public String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    return meta.getPath();
  }

  @Override
  public String readUserBlock(long sessionId, long userId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, WorkerOutOfSpaceException {
    BlockMeta blockMeta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    mBlockStore.readSharedBlock(blockMeta, userId);
    return blockMeta.getPath();
  }

  @Override
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  @Override
  public BlockReader readUserBlockRemote(long sessionId, long userId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException,
      WorkerOutOfSpaceException {
    return mBlockStore.getUserBlockReader(sessionId, userId, blockId, lockId);
  }

  @Override
  public BlockReader readUfsBlock(long sessionId, long blockId, long offset)
      throws BlockDoesNotExistException, IOException {
    return mUnderFileSystemBlockStore.getBlockReader(sessionId, blockId, offset);
  }

  @Override
  public BlockReader readUserUfsBlock(long sessionId, long userId, long blockId, long offset)
      throws BlockDoesNotExistException, IOException {
    return mUnderFileSystemBlockStore.getBlockReader(sessionId, blockId, offset);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void removeUserBlock(long sessionId, long userId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mBlockStore.removeUserBlock(sessionId, userId, blockId);
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void requestUserSpace(long sessionId, long userId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestUserSpace(sessionId, userId, blockId, additionalBytes);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mBlockStore.unlockBlock(lockId);
  }

  @Override
  public void unlockUserBlock(long lockId) throws BlockDoesNotExistException {
    mBlockStore.unlockUserBlock(lockId);
  }

  @Override
  // TODO(calvin): Remove when lock and reads are separate operations.
  public boolean unlockBlock(long sessionId, long blockId) {
    return mBlockStore.unlockBlock(sessionId, blockId);
  }

  @Override
  public boolean unlockUserBlock(long sessionId, long blockId) {
    return mBlockStore.unlockUserBlock(sessionId, blockId);
  }

  @Override
  public void sessionHeartbeat(long sessionId) {
    mSessions.sessionHeartbeat(sessionId);
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return mFileSystemMasterClient.getFileInfo(fileId);
  }

  @Override
  public boolean openUfsBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    if (!options.hasUfsPath() && options.hasBlockInUfsTier() && options.getBlockInUfsTier()) {
      // This is a fallback UFS block read. Reset the UFS block path according to the UfsBlock flag.
      UfsManager.UfsClient ufsClient;
      try {
        ufsClient = mUfsManager.get(options.getMountId());
      } catch (alluxio.exception.status.NotFoundException
          | alluxio.exception.status.UnavailableException e) {
        LOG.warn("Can not open UFS block: mount id {} not found",
            options.getMountId(), e.getMessage());
        return false;
      }
      options = options.toBuilder().setUfsPath(
          alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, blockId)).build();
    }
    return mUnderFileSystemBlockStore.acquireAccess(sessionId, blockId, options);
  }

  @Override
  public boolean openUserUfsBlock(long sessionId, long userId, long blockId,
      Protocol.OpenUfsBlockOptions options) throws BlockAlreadyExistsException {
    if (!options.hasUfsPath() && options.hasBlockInUfsTier() && options.getBlockInUfsTier()) {
      // This is a fallback UFS block read. Reset the UFS block path according to the UfsBlock flag.
      UfsManager.UfsClient ufsClient;
      try {
        ufsClient = mUfsManager.get(options.getMountId());
      } catch (alluxio.exception.status.NotFoundException
          | alluxio.exception.status.UnavailableException e) {
        LOG.warn("Can not open UFS block: mount id {} not found",
            options.getMountId(), e.getMessage());
        return false;
      }
      options = options.toBuilder().setUfsPath(
          alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, blockId)).build();
    }
    return mUnderFileSystemBlockStore.acquireUserAccess(sessionId, blockId, options, userId);
  }

  @Override
  public void closeUfsBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    try {
      mUnderFileSystemBlockStore.closeReaderOrWriter(sessionId, blockId);
      if (mBlockStore.getTempBlockMeta(sessionId, blockId) != null) {
        try {
          commitBlock(sessionId, blockId, false);
        } catch (BlockDoesNotExistException e) {
          // This can only happen if the session is expired. Ignore this exception if that happens.
          LOG.warn("Block {} does not exist while being committed.", blockId);
        } catch (InvalidWorkerStateException e) {
          // This can happen if there are multiple sessions writing to the same block.
          // BlockStore#getTempBlockMeta does not check whether the temp block belongs to
          // the sessionId.
          LOG.debug("Invalid worker state while committing block.", e);
        }
      }
    } finally {
      mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
    }
  }
  @Override
  public void closeUserUfsBlock(long sessionId, long userId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, IOException,
      WorkerOutOfSpaceException {
    try {
      mUnderFileSystemBlockStore.closeReaderOrWriter(sessionId, blockId);
      if (mBlockStore.getTempBlockMeta(sessionId, blockId) != null) {
        try {
          commitUserBlock(sessionId, userId, blockId, false);
        } catch (BlockDoesNotExistException e) {
          // This can only happen if the session is expired. Ignore this exception if that happens.
          LOG.warn("Block {} does not exist while being committed.", blockId);
        } catch (InvalidWorkerStateException e) {
          // This can happen if there are multiple sessions writing to the same block.
          // BlockStore#getTempBlockMeta does not check whether the temp block belongs to
          // the sessionId.
          LOG.debug("Invalid worker state while committing block.", e);
        }
      }
    } finally {
      mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
    }
  }

  @Override
  public String generateUserSpaceReport() {
    return mUserSpaceReporter.generateUserSpaceReport();
  }
  @Override
  public long getTotalCapacityBytes() {
    return mBlockStore.getTotalCapacityBytes();
  }

  @Override
  public long getTotalAvailableBytes() {
    return mBlockStore.getTotalAvailableBytes();
  }

  @Override
  public Map<Long, UserStorageInfo> getAllUserStorageInfo() {
    return mBlockStore.getAllUserStorageInfo();
  }

  @Override
  public void cleanupSession(long sessionId) {
    mBlockStore.cleanupSession(sessionId);
    mUnderFileSystemBlockStore.cleanupSession(sessionId);
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceWorkerMetricsServlet}.
   */
  @SuppressWarnings("JavadocReference")
  @ThreadSafe
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String BLOCKS_CACHED = "BlocksCached";
    public static final String TIER = "Tier";

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the block worker handle
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_TOTAL),
          () -> blockWorker.getStoreMeta().getCapacityBytes());

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_USED),
          () -> blockWorker.getStoreMeta().getUsedBytes());

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_FREE),
          () -> blockWorker.getStoreMeta().getCapacityBytes() - blockWorker.getStoreMeta()
                      .getUsedBytes());

      StorageTierAssoc assoc = blockWorker.getStoreMeta().getStorageTierAssoc();
      for (int i = 0; i < assoc.size(); i++) {
        String tier = assoc.getAlias(i);
        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_TOTAL + TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_USED + TIER + tier),
            () -> blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_FREE + TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L)
                - blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));
      }
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(BLOCKS_CACHED),
          () -> blockWorker.getStoreMetaFull().getNumberOfBlocks());
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * StorageChecker periodically checks the health of each storage path and report missing blocks to
   * {@link BlockWorker}.
   */
  @NotThreadSafe
  public final class StorageChecker implements HeartbeatExecutor {

    @Override
    public void heartbeat() {
      try {
        if (mBlockStore.checkStorage()) {
          mSpaceReserver.updateStorageInfo();
        }
      } catch (Exception e) {
        LOG.warn("Failed to check storage", e.getMessage());
        LOG.debug("Exception: ", e);
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
