// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface DeletePOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.DeletePOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional bool recursive = 1;</code>
   */
  boolean hasRecursive();
  /**
   * <code>optional bool recursive = 1;</code>
   */
  boolean getRecursive();

  /**
   * <code>optional bool alluxioOnly = 2;</code>
   */
  boolean hasAlluxioOnly();
  /**
   * <code>optional bool alluxioOnly = 2;</code>
   */
  boolean getAlluxioOnly();

  /**
   * <code>optional bool unchecked = 3;</code>
   */
  boolean hasUnchecked();
  /**
   * <code>optional bool unchecked = 3;</code>
   */
  boolean getUnchecked();

  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}
