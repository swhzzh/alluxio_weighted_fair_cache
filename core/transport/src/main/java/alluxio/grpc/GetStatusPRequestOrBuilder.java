// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface GetStatusPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.GetStatusPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the path of the file or directory 
   * </pre>
   *
   * <code>optional string path = 1;</code>
   */
  boolean hasPath();
  /**
   * <pre>
   ** the path of the file or directory 
   * </pre>
   *
   * <code>optional string path = 1;</code>
   */
  java.lang.String getPath();
  /**
   * <pre>
   ** the path of the file or directory 
   * </pre>
   *
   * <code>optional string path = 1;</code>
   */
  com.google.protobuf.ByteString
      getPathBytes();

  /**
   * <code>optional .alluxio.grpc.file.GetStatusPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.GetStatusPOptions options = 2;</code>
   */
  alluxio.grpc.GetStatusPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.GetStatusPOptions options = 2;</code>
   */
  alluxio.grpc.GetStatusPOptionsOrBuilder getOptionsOrBuilder();
}
