// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface StopSyncPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.StopSyncPRequest)
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
   * <code>optional .alluxio.grpc.file.StopSyncPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.StopSyncPOptions options = 2;</code>
   */
  alluxio.grpc.StopSyncPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.StopSyncPOptions options = 2;</code>
   */
  alluxio.grpc.StopSyncPOptionsOrBuilder getOptionsOrBuilder();
}
