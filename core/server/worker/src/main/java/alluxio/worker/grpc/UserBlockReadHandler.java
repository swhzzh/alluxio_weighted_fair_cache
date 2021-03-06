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

package alluxio.worker.grpc;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ReadResponse;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.UnderFileSystemBlockReader;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block read request. Check more information in {@link AbstractReadHandler}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class UserBlockReadHandler extends AbstractReadHandler<BlockReadRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      ServerConfiguration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** The Block Worker. */
  private final BlockWorker mWorker;

  private final boolean mDomainSocketEnabled;


  private final long mUserId;
  /**
   * The data reader to read from a local block worker.
   */
  @NotThreadSafe
  public final class BlockDataReader extends DataReader {
    /** The Block Worker. */
    private final BlockWorker mWorker;
    /** An object storing the mapping of tier aliases to ordinals. */
    private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
    private long mUserId;
    BlockDataReader(BlockReadRequestContext context, StreamObserver<ReadResponse> response,
        BlockWorker blockWorker, long userId) {
      super(context, response);
      mWorker = blockWorker;
      mUserId = userId;
    }

    @Override
    protected void completeRequest(BlockReadRequestContext context) throws Exception {
      BlockReader reader = context.getBlockReader();
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {} with error {}.",
              context.getRequest().getId(), e.getMessage());
        }
      }
      if (!mWorker.unlockUserBlock(context.getRequest().getSessionId(), context.getRequest().getId())) {
        if (reader != null) {
          mWorker.closeUserUfsBlock(context.getRequest().getSessionId(), mUserId, context.getRequest().getId());
          context.setBlockReader(null);
        }
      }
    }

    @Override
    protected DataBuffer getDataBuffer(BlockReadRequestContext context,
        StreamObserver<ReadResponse> response, long offset, int len) throws Exception {
      openUserBlock(context, response);
      BlockReader blockReader = context.getBlockReader();
      Preconditions.checkState(blockReader != null);
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(len, len);
      try {
        while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
        }
        return new NettyDataBuffer(buf);
      } catch (Throwable e) {
        buf.release();
        throw e;
      }
    }

    /**
     * Opens the block if it is not open.
     *
     * @param response the read response stream
     * @throws Exception if it fails to open the block
     */
    private void openUserBlock(BlockReadRequestContext context, StreamObserver<ReadResponse> response)
        throws Exception {
      if (context.getBlockReader() != null) {
        return;
      }
      BlockReadRequest request = context.getRequest();
      // TODO(calvin): Update the locking logic so this can be done better
      // Cache_Promote?????????block?????????alluxio?????????, ??????????????????, ?????????????????????, ????????????move
      // ???????????????, ??????readtype??????CACHE, ?????????promote
      /*if (request.isPromote()) {
        try {
          mWorker.moveUserBlock(request.getSessionId(), mUserId, request.getId(), mStorageTierAssoc.getAlias(0));
        } catch (BlockDoesNotExistException e) {
          LOG.debug("Block {} to promote does not exist in Alluxio: {}", request.getId(),
              e.getMessage());
        } catch (Exception e) {
          LOG.warn("Failed to promote block {}: {}", request.getId(), e.getMessage());
        }
      }*/
      // ?????????client???worker????????????, ???????????????, ???worker???client??????????????????worker, ??????????????????????????????client?????????worker?????????
      // ???????????????????????????worker, ??????client???worker??????, ??????????????????
      int retryInterval = Constants.SECOND_MS;
      RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);
      while (retryPolicy.attempt()) {
        long lockId;
        if (request.isPersisted() || (request.getOpenUfsBlockOptions() != null && request
            .getOpenUfsBlockOptions().hasBlockInUfsTier() && request.getOpenUfsBlockOptions()
            .getBlockInUfsTier())) {
          lockId = mWorker.lockUserBlockNoException(request.getSessionId(), mUserId, request.getId());
        } else {
          lockId = mWorker.lockUserBlock(request.getSessionId(), mUserId, request.getId());
        }
        if (lockId != BlockLockManager.INVALID_LOCK_ID) {
          try {
            BlockReader reader =
                mWorker.readUserBlockRemote(request.getSessionId(), mUserId, request.getId(), lockId);
            context.setBlockReader(reader);
            mWorker.accessUserBlock(request.getSessionId(), mUserId, request.getId());
            ((FileChannel) reader.getChannel()).position(request.getStart());
            return;
          }
          catch (BlockDoesNotExistException e){
            LOG.info("block {} doesn't exist, try to read from ufs", request.getId());
            mWorker.unlockUserBlock(lockId);
          }
          catch (WorkerOutOfSpaceException e){
            LOG.info("user {} doesn't hava enough space for block {}, try to read from ufs", mUserId, request.getId());
            mWorker.unlockUserBlock(lockId);
          }
          catch (Exception e) {
            mWorker.unlockUserBlock(lockId);
            throw e;
          }
        }

        // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
        Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
        if (mWorker.openUserUfsBlock(request.getSessionId(), mUserId, request.getId(),
            Protocol.OpenUfsBlockOptions.parseFrom(openUfsBlockOptions.toByteString()))) {
          try {
            BlockReader reader =
                mWorker.readUserUfsBlock(request.getSessionId(), mUserId, request.getId(), request.getStart());
            AlluxioURI ufsMountPointUri =
                ((UnderFileSystemBlockReader) reader).getUfsMountPointUri();
            String ufsString = MetricsSystem.escape(ufsMountPointUri);
            String counterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_READ_UFS,
                WorkerMetrics.TAG_UFS, ufsString);
            context.setBlockReader(reader);
            context.setCounter(MetricsSystem.counter(counterName));
            String meterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT,
                WorkerMetrics.TAG_UFS, ufsString);
            context.setMeter(MetricsSystem.meter(meterName));
            return;
          } catch (Exception e) {
            // TODO(binfan): remove the closeUfsBlock here as the exception will be handled in
            // AbstractReadHandler. Current approach to use context.blockReader as a flag is a
            // workaround.
            mWorker.closeUserUfsBlock(request.getSessionId(), mUserId, request.getId());
            context.setBlockReader(null);
            throw e;
          }
        }
      }
      throw new UnavailableException(ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE
          .getMessage(request.getId(), request.getOpenUfsBlockOptions().getUfsPath()));
    }
  }

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link DataReader}s
   * @param blockWorker the block worker
   * @param responseObserver the response observer of the gRPC stream
   * @param userInfo the authenticated user info
   * @param domainSocketEnabled whether reading block over domain socket
   */
  public UserBlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
      StreamObserver<ReadResponse> responseObserver, AuthenticatedUserInfo userInfo,
      boolean domainSocketEnabled, long userId) {
    super(executorService, responseObserver, userInfo);
    mWorker = blockWorker;
    mDomainSocketEnabled = domainSocketEnabled;
    mUserId = userId;
  }

  @Override
  protected BlockReadRequestContext createRequestContext(alluxio.grpc.ReadRequest request) {
    BlockReadRequestContext context = new BlockReadRequestContext(request);
    if (mDomainSocketEnabled) {
      context.setCounter(MetricsSystem.counter(WorkerMetrics.BYTES_READ_DOMAIN));
      context.setMeter(MetricsSystem.meter(WorkerMetrics.BYTES_READ_DOMAIN_THROUGHPUT));
    } else {
      context.setCounter(MetricsSystem.counter(WorkerMetrics.BYTES_READ_ALLUXIO));
      context.setMeter(MetricsSystem.meter(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT));
    }
    return context;
  }

  @Override
  protected DataReader createDataReader(BlockReadRequestContext context,
      StreamObserver<ReadResponse> response) {
    return new BlockDataReader(context, response, mWorker, mUserId);
  }
}
