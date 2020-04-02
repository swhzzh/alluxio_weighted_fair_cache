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

import alluxio.RpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GenerateUserSpaceReportRequest;
import alluxio.grpc.GenerateUserSpaceReportResponse;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequestMarshaller;
import alluxio.grpc.WriteResponse;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.AsyncCacheRequestManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Server side implementation of the gRPC BlockWorker interface.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class BlockWorkerImpl extends BlockWorkerGrpc.BlockWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerImpl.class);

  private static final boolean ZERO_COPY_ENABLED =
      ServerConfiguration.getBoolean(PropertyKey.WORKER_NETWORK_ZEROCOPY_ENABLED);
  private WorkerProcess mWorkerProcess;
  private final AsyncCacheRequestManager mRequestManager;
  private ReadResponseMarshaller mReadResponseMarshaller = new ReadResponseMarshaller();
  private WriteRequestMarshaller mWriteRequestMarshaller = new WriteRequestMarshaller();
  private final boolean mDomainSocketEnabled;
  private Map<String, Long> mUserNameToUserIdMap = new HashMap<>();
  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   * @param fsContext context used to read blocks
   * @param domainSocketEnabled is using domain sockets
   */
  public BlockWorkerImpl(WorkerProcess workerProcess, FileSystemContext fsContext,
      boolean domainSocketEnabled) {
    mWorkerProcess = workerProcess;
    mRequestManager = new AsyncCacheRequestManager(
        GrpcExecutors.ASYNC_CACHE_MANAGER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class),
        fsContext);
    mDomainSocketEnabled = domainSocketEnabled;
    // preload some user data for test
    for (long i = 1; i <= 4 ; i++) {
      mUserNameToUserIdMap.put("swh" + i, i);
    }
  }

  /**
   * @return a map of gRPC methods with overridden descriptors
   */
  public Map<MethodDescriptor, MethodDescriptor> getOverriddenMethodDescriptors() {
    if (ZERO_COPY_ENABLED) {
      return ImmutableMap.of(
          BlockWorkerGrpc.getReadBlockMethod(),
          BlockWorkerGrpc.getReadBlockMethod().toBuilder()
              .setResponseMarshaller(mReadResponseMarshaller).build(),
          BlockWorkerGrpc.getWriteBlockMethod(),
          BlockWorkerGrpc.getWriteBlockMethod().toBuilder()
              .setRequestMarshaller(mWriteRequestMarshaller).build());
    }
    return Collections.emptyMap();
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    CallStreamObserver<ReadResponse> callStreamObserver =
        (CallStreamObserver<ReadResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      callStreamObserver =
          new DataMessageServerStreamObserver<>(callStreamObserver, mReadResponseMarshaller);
    }
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to readBlock", userInfo.getAuthorizedUserName());
    UserBlockReadHandler readHandler = new UserBlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mWorkerProcess.getWorker(BlockWorker.class), callStreamObserver, userInfo
        , mDomainSocketEnabled, userId);
    callStreamObserver.setOnReadyHandler(readHandler::onReady);
    return readHandler;
  }

  @Override
  public StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
      StreamObserver<WriteResponse> responseObserver) {
    ServerCallStreamObserver<WriteResponse> serverResponseObserver =
        (ServerCallStreamObserver<WriteResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      responseObserver =
          new DataMessageServerRequestObserver<>(responseObserver, mWriteRequestMarshaller, null);
    }
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to writeBlock", userInfo.getAuthorizedUserName());
    UserDelegationWriteHandler handler = new UserDelegationWriteHandler(mWorkerProcess, responseObserver,
        userInfo, mDomainSocketEnabled, userId);
    serverResponseObserver.setOnCancelHandler(handler::onCancel);
    return handler;
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to openLocalBlock", userInfo.getAuthorizedUserName());
    return new UserShortCircuitBlockReadHandler(
        mWorkerProcess.getWorker(BlockWorker.class), responseObserver, userInfo, userId);

  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to createLocalBlock", userInfo.getAuthorizedUserName());
    UserShortCircuitBlockWriteHandler handler = new UserShortCircuitBlockWriteHandler(
        mWorkerProcess.getWorker(BlockWorker.class), responseObserver, userInfo, userId);
    ServerCallStreamObserver<CreateLocalBlockResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<CreateLocalBlockResponse>) responseObserver;
    serverCallStreamObserver.setOnCancelHandler(handler::onCancel);
    return handler;

  }

  @Override
  public void asyncCache(AsyncCacheRequest request,
      StreamObserver<AsyncCacheResponse> responseObserver) {
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to asyncCache", userInfo.getAuthorizedUserName());
    Long finalUserId = userId;
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<AsyncCacheResponse>) () -> {
      mRequestManager.submitUserRequest(request, finalUserId);
      return AsyncCacheResponse.getDefaultInstance();
    }, "asyncCache", "request=%s", responseObserver, request);

  }

  @Override
  public void removeBlock(RemoveBlockRequest request,
      StreamObserver<RemoveBlockResponse> responseObserver) {
    AuthenticatedUserInfo userInfo = getAuthenticatedUserInfo();
    Long userId = mUserNameToUserIdMap.get(userInfo.getAuthorizedUserName());
    if (userId == null){
      userId = -1L;
    }
    LOG.info("user {} tries to removeBlock", userInfo.getAuthorizedUserName());
    long sessionId = IdUtils.createSessionId();
    Long finalUserId = userId;
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<RemoveBlockResponse>) () -> {
      mWorkerProcess.getWorker(BlockWorker.class).removeUserBlock(sessionId, finalUserId, request.getBlockId());
      return RemoveBlockResponse.getDefaultInstance();
    }, "removeBlock", "request=%s", responseObserver, request);

  }

  @Override
  public void moveBlock(MoveBlockRequest request,
      StreamObserver<MoveBlockResponse> responseObserver) {
    long sessionId = IdUtils.createSessionId();
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<MoveBlockResponse>) () -> {
      mWorkerProcess.getWorker(BlockWorker.class).moveBlockToMedium(sessionId,
          request.getBlockId(), request.getMediumType());
      return MoveBlockResponse.getDefaultInstance();
    }, "moveBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void generateUserSpaceReport(GenerateUserSpaceReportRequest request,
      StreamObserver<GenerateUserSpaceReportResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GenerateUserSpaceReportResponse>) () -> {

      String userSpaceReport = mWorkerProcess.getWorker(BlockWorker.class).generateUserSpaceReport();
      return GenerateUserSpaceReportResponse.newBuilder().setUserSpaceReport(userSpaceReport).build();
    }, "generateUserSpaceReport", "request=%s", responseObserver, request);
  }

  /**
   * @return {@link AuthenticatedUserInfo} that defines the user that has been authorized
   */
  private AuthenticatedUserInfo getAuthenticatedUserInfo() {
    try {
      if (SecurityUtils.isAuthenticationEnabled(ServerConfiguration.global())) {
        return new AuthenticatedUserInfo(
            AuthenticatedClientUser.getClientUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getConnectionUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getAuthMethod(ServerConfiguration.global()));
      } else {
        return new AuthenticatedUserInfo();
      }
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED.withDescription(e.toString()).asRuntimeException();
    }
  }
}
