// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface StartSyncPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.StartSyncPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string path = 1;</code>
   */
  boolean hasPath();
  /**
   * <code>optional string path = 1;</code>
   */
  java.lang.String getPath();
  /**
   * <code>optional string path = 1;</code>
   */
  com.google.protobuf.ByteString
      getPathBytes();

  /**
   * <code>optional .alluxio.grpc.file.StartSyncPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.StartSyncPOptions options = 2;</code>
   */
  alluxio.grpc.StartSyncPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.StartSyncPOptions options = 2;</code>
   */
  alluxio.grpc.StartSyncPOptionsOrBuilder getOptionsOrBuilder();
}
