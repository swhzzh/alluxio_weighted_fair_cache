package alluxio.worker.block;

import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.worker.block.meta.UserStorageInfo;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserSpaceReporter implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(UserSpaceReporter.class);
  /** The block worker the space reserver monitors. */
  private final BlockWorker mBlockWorker;

  private FileWriter mFileWriter;
  private String mCurDir = System.getProperty("user.dir");
  private Closer mCloser;
  private Map<Long, String> mBlockIdToFileNameMap = new HashMap<>();

  public UserSpaceReporter(BlockWorker blockWorker){
    mBlockWorker = blockWorker;
    mCloser = Closer.create();
    try {
      //LOG.info(mCurDir + "/logs/user/user_space_report.log");
      File file = new File(mCurDir + "/logs/user/user_space_report.log");
      if (!file.exists()){
        file.createNewFile();
      }
      mFileWriter = mCloser.register(new FileWriter(file));
      LOG.info("user_space_report.log was created successfully!");
    } catch (IOException e) {
      e.printStackTrace();
    }

    //preload
    mBlockIdToFileNameMap.put(33554432L, "test1_10M.txt");
    mBlockIdToFileNameMap.put(67108864L, "test1_20M.txt");
    mBlockIdToFileNameMap.put(100663296L, "test1_30M.txt");
    mBlockIdToFileNameMap.put(134217728L, "test1_40M.txt");
    mBlockIdToFileNameMap.put(150994944L, "test1_50M.txt");
    mBlockIdToFileNameMap.put(184549376L, "test2_10M.txt");
    mBlockIdToFileNameMap.put(201326592L, "test2_20M.txt");
    mBlockIdToFileNameMap.put(218103808L, "test2_30M.txt");
    mBlockIdToFileNameMap.put(234881024L, "test2_40M.txt");
    mBlockIdToFileNameMap.put(251658240L, "test2_50M.txt");
    mBlockIdToFileNameMap.put(268435456L, "test3_10M.txt");
    mBlockIdToFileNameMap.put(285212672L, "test3_20M.txt");
    mBlockIdToFileNameMap.put(301989888L, "test3_30M.txt");
    mBlockIdToFileNameMap.put(318767104L, "test3_40M.txt");
    mBlockIdToFileNameMap.put(335544320L, "test3_50M.txt");
    mBlockIdToFileNameMap.put(352321536L, "test4_10M.txt");
    mBlockIdToFileNameMap.put(369098752L, "test4_20M.txt");
    mBlockIdToFileNameMap.put(385875968L, "test4_30M.txt");
    mBlockIdToFileNameMap.put(402653184L, "test4_40M.txt");
    mBlockIdToFileNameMap.put(419430400L, "test4_50M.txt");
  }

  public String generateUserSpaceReport(){
    StringBuilder result = new StringBuilder();
    // log info some information
    long totalCapacityBytes = mBlockWorker.getTotalCapacityBytes();
    long totalAvailableBytes = mBlockWorker.getTotalAvailableBytes();
    Map<Long, UserStorageInfo> userStorageInfoMap = mBlockWorker.getAllUserStorageInfo();
      result.append("Total capacity is ").append(String.valueOf(totalCapacityBytes / (1024 * 1024))).append("mb\n")
          .append("Total available is ").append(String.valueOf(totalAvailableBytes / (1024 * 1024))).append("mb\n");
      for (Map.Entry<Long, UserStorageInfo> entry : userStorageInfoMap.entrySet()) {
        long userId = entry.getKey();
        UserStorageInfo userStorageInfo = entry.getValue();
        double weight = userStorageInfo.getWeight();
        long usedBytes = userStorageInfo.getUsedBytes();
        result.append("User swh").append(String.valueOf(userId)).append("(weight").append(String.valueOf(weight))
            .append(") uses ").append(String.valueOf(usedBytes / (1024.0 * 1024))).append("mb\nIts blocks are :\n");
        int count = 0;
        for (Map.Entry<Long, Long> blockEntry : userStorageInfo.getBlockIdToUsedSpaceMap().entrySet()) {
          long blockId = blockEntry.getKey();
          long spaceUsed = blockEntry.getValue();
          String fileName = mBlockIdToFileNameMap.get(blockId);
          if (fileName == null){
            fileName = String.valueOf(blockId);
          }
          result.append(fileName).append("(").append(String.valueOf(spaceUsed / (1024.0 * 1024)))
              .append("mb), ");
          count++;
          if (count % 4 == 0){
            result.append("\n");
          }
        }
        result.append("\n");
      }
      return result.toString();
  }


  @Override
  public void heartbeat() throws InterruptedException {
    // log info some information
    long totalCapacityBytes = mBlockWorker.getTotalCapacityBytes();
    long totalAvailableBytes = mBlockWorker.getTotalAvailableBytes();
    Map<Long, UserStorageInfo> userStorageInfoMap = mBlockWorker.getAllUserStorageInfo();
    try {
      mFileWriter.append("Total capacity is ").append(String.valueOf(totalCapacityBytes / (1024 * 1024))).append("mb\n")
          .append("Total available is ").append(String.valueOf(totalAvailableBytes / (1024 * 1024))).append("mb\n");
      for (Map.Entry<Long, UserStorageInfo> entry : userStorageInfoMap.entrySet()) {
        long userId = entry.getKey();
        UserStorageInfo userStorageInfo = entry.getValue();
        double weight = userStorageInfo.getWeight();
        long usedBytes = userStorageInfo.getUsedBytes();
        mFileWriter.append("User swh").append(String.valueOf(userId)).append("(weight").append(String.valueOf(weight))
            .append(") uses ").append(String.valueOf(usedBytes / (1024 * 1024))).append("mb\nIts blocks are :\n");
        int count = 0;
        for (Map.Entry<Long, Long> blockEntry : userStorageInfo.getBlockIdToUsedSpaceMap().entrySet()) {
          long blockId = blockEntry.getKey();
          long spaceUsed = blockEntry.getValue();
          mFileWriter.append(String.valueOf(blockId)).append("(").append(String.valueOf(spaceUsed / (1024 * 1024)))
              .append("mb), ");
          count++;
          if (count % 4 == 0){
            mFileWriter.append("\n");
          }
        }
        mFileWriter.append("\n");
      }

      mFileWriter.flush();
    }catch (IOException e){
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      mFileWriter.flush();
      mCloser.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
