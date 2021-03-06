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

package alluxio.client.file;

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CheckConsistencyPRequest;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.GetFilePathPRequest;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetNewBlockIdForFilePOptions;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetSyncPathListPRequest;
import alluxio.grpc.GetWeightsRequest;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.ReverseResolvePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAclPRequest;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.SetUserWeightsRequest;
import alluxio.grpc.StartSyncPRequest;
import alluxio.grpc.StopSyncPRequest;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UpdateMountPRequest;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.master.MasterClientContext;
import alluxio.security.authorization.AclEntry;
import alluxio.util.FileSystemOptions;
import alluxio.wire.SyncPointInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the file system master, used by alluxio clients.
 *
 */
@ThreadSafe
public final class RetryHandlingFileSystemMasterClient extends AbstractMasterClient
    implements FileSystemMasterClient {
  private FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub mClient =
      null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param conf master client configuration
   */
  public RetryHandlingFileSystemMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = FileSystemMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyPOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<String> inconsistentPaths = mClient.checkConsistency(CheckConsistencyPRequest
          .newBuilder().setPath(getTransportPath(path)).setOptions(options).build())
          .getInconsistentPathsList();
      List<AlluxioURI> inconsistentUris = new ArrayList<>(inconsistentPaths.size());
      for (String inconsistentPath : inconsistentPaths) {
        inconsistentUris.add(new AlluxioURI(inconsistentPath));
      }
      return inconsistentUris;
    }, "CheckConsistency");
  }

  @Override
  public void createDirectory(final AlluxioURI path,
      final CreateDirectoryPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.createDirectory(CreateDirectoryPRequest.newBuilder()
            .setPath(getTransportPath(path)).setOptions(options).build()),
        "CreateDirectory");
  }

  @Override
  public URIStatus createFile(final AlluxioURI path, final CreateFilePOptions options)
      throws AlluxioStatusException {
    return retryRPC(
        () -> new URIStatus(GrpcUtils.fromProto(mClient.createFile(CreateFilePRequest.newBuilder()
            .setPath(getTransportPath(path)).setOptions(options).build()).getFileInfo())),
        "CreateFile");
  }

  @Override
  public void completeFile(final AlluxioURI path, final CompleteFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.completeFile(CompleteFilePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), "CompleteFile");
  }

  @Override
  public void delete(final AlluxioURI path, final DeletePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.remove(DeletePRequest.newBuilder().setPath(getTransportPath(path))
        .setOptions(options).build()), "Delete");
  }

  @Override
  public void free(final AlluxioURI path, final FreePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.free(FreePRequest.newBuilder().setPath(getTransportPath(path))
        .setOptions(options).build()), "Free");
  }

  @Override
  public String getFilePath(long fileId) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getFilePath(GetFilePathPRequest
            .newBuilder().setFileId(fileId).build()).getPath(), "GetFilePath");
  }

  @Override
  public URIStatus getStatus(final AlluxioURI path, final GetStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> new URIStatus(GrpcUtils
        .fromProto(mClient.getStatus(GetStatusPRequest.newBuilder().setPath(getTransportPath(path))
            .setOptions(options).build()).getFileInfo())),
        "GetStatus");
  }

  @Override
  public synchronized List<SyncPointInfo> getSyncPathList() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getSyncPathList(GetSyncPathListPRequest.getDefaultInstance())
        .getSyncPathsList().stream().map(x -> alluxio.wire.SyncPointInfo.fromProto(x))
        .collect(Collectors.toList()), "GetSyncPathList");
  }

  @Override
  public long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
    return retryRPC(
        () -> mClient.getNewBlockIdForFile(
            GetNewBlockIdForFilePRequest.newBuilder().setPath(getTransportPath(path))
                .setOptions(GetNewBlockIdForFilePOptions.newBuilder().build()).build())
            .getId(),
        "GetNewBlockIdForFile");
  }

  @Override
  public Map<String, alluxio.wire.MountPointInfo> getMountTable() throws AlluxioStatusException {
    return retryRPC(() -> {
      Map<String, alluxio.wire.MountPointInfo> mountTableWire = new HashMap<>();
      for (Map.Entry<String, alluxio.grpc.MountPointInfo> entry : mClient
          .getMountTable(GetMountTablePRequest.newBuilder().build()).getMountPointsMap()
          .entrySet()) {
        mountTableWire.put(entry.getKey(), GrpcUtils.fromProto(entry.getValue()));
      }
      return mountTableWire;
    }, "GetMountTable");
  }

  @Override
  public List<URIStatus> listStatus(final AlluxioURI path, final ListStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      List<URIStatus> result = new ArrayList<>();
      mClient
          .listStatus(ListStatusPRequest.newBuilder().setPath(getTransportPath(path))
              .setOptions(options).build())
          .forEachRemaining(
              (pListStatusResponse) -> result.addAll(pListStatusResponse.getFileInfosList().stream()
                  .map((pFileInfo) -> new URIStatus(GrpcUtils.fromProto(pFileInfo)))
                  .collect(Collectors.toList())));
      return result;
    }, "ListStatus");
  }

  @Override
  public void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.mount(MountPRequest.newBuilder().setAlluxioPath(alluxioPath.toString())
            .setUfsPath(ufsPath.toString()).setOptions(options).build()),
        "Mount");
  }

  @Override
  public void updateMount(final AlluxioURI alluxioPath, final MountPOptions options)
      throws AlluxioStatusException {
    retryRPC(
        () -> mClient.updateMount(UpdateMountPRequest.newBuilder()
            .setAlluxioPath(alluxioPath.toString())
            .setOptions(options).build()),
        "UpdateMount");
  }

  @Override
  public void rename(final AlluxioURI src, final AlluxioURI dst)
      throws AlluxioStatusException {
    rename(src, dst, FileSystemOptions.renameDefaults(mContext.getClusterConf()));
  }

  @Override
  public void rename(final AlluxioURI src, final AlluxioURI dst,
      final RenamePOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.rename(RenamePRequest.newBuilder().setPath(getTransportPath(src))
        .setDstPath(getTransportPath(dst)).setOptions(options).build()), "Rename");
  }

  @Override
  public AlluxioURI reverseResolve(final AlluxioURI ufsUri) throws AlluxioStatusException {
    return retryRPC(() -> new AlluxioURI(mClient.reverseResolve(ReverseResolvePRequest.newBuilder()
        .setUfsUri(ufsUri.toString()).build()).getAlluxioPath()), "ReverseResolve");
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.setAcl(
        SetAclPRequest.newBuilder().setPath(getTransportPath(path)).setAction(action)
            .addAllEntries(entries.stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
            .setOptions(options).build()),
        "SetAcl");
  }

  @Override
  public void setAttribute(final AlluxioURI path, final SetAttributePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.setAttribute(SetAttributePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), "SetAttribute");
  }

  @Override
  public void scheduleAsyncPersist(final AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.scheduleAsyncPersistence(ScheduleAsyncPersistencePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), "ScheduleAsyncPersist");
  }

  @Override
  public synchronized void startSync(final AlluxioURI path) throws AlluxioStatusException {
    retryRPC(
        () -> mClient
            .startSync(StartSyncPRequest.newBuilder().setPath(getTransportPath(path)).build()),
        "StartSync");
  }

  @Override
  public synchronized void stopSync(final AlluxioURI path) throws AlluxioStatusException {
    retryRPC(
        () -> mClient
            .stopSync(StopSyncPRequest.newBuilder().setPath(getTransportPath(path)).build()),
        "StopSync");
  }

  @Override
  public void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
    retryRPC(() -> mClient
        .unmount(UnmountPRequest.newBuilder().setAlluxioPath(getTransportPath(alluxioPath))
            .setOptions(UnmountPOptions.newBuilder().build()).build()),
        "Unmount");
  }

  @Override
  public void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModePOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.updateUfsMode(UpdateUfsModePRequest.newBuilder()
            .setUfsPath(ufsUri.getRootPath()).setOptions(options).build()),
        "UpdateUfsMode");
  }

  @Override
  public void setUserWeights(long userId, Map<Long, Double> weights) throws AlluxioStatusException {
    retryRPC(() -> mClient.setUserWeights(SetUserWeightsRequest.newBuilder()
            .setUserId(userId).putAllWeights(weights).build()),
        "setUserWeights");
  }

  @Override
  public Map<Long, Double> getWeights(long userId) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getWeights(GetWeightsRequest.newBuilder()
        .setUserId(userId).build()), "getWeights").getWeightsMap();
  }

  /**
   * Gets the path that will be transported to master.
   *
   * @param uri uri
   * @return transport path
   */
  private static String getTransportPath(AlluxioURI uri) {
    if (uri.hasScheme() && !uri.getScheme().equals(Constants.SCHEME)) {
      // Return full URI for non-Alluxio path.
      return uri.toString();
    } else {
      // Scheme-less URIs are assumed to be Alluxio paths
      // and getPath() is used to avoid string conversion.
      return uri.getPath();
    }
  }
}
