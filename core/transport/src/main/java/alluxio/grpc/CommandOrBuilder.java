// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

public interface CommandOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.Command)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.CommandType commandType = 1;</code>
   */
  boolean hasCommandType();
  /**
   * <code>optional .alluxio.grpc.CommandType commandType = 1;</code>
   */
  alluxio.grpc.CommandType getCommandType();

  /**
   * <code>repeated int64 data = 2;</code>
   */
  java.util.List<java.lang.Long> getDataList();
  /**
   * <code>repeated int64 data = 2;</code>
   */
  int getDataCount();
  /**
   * <code>repeated int64 data = 2;</code>
   */
  long getData(int index);
}
