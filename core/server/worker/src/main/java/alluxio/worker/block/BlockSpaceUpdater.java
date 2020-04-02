package alluxio.worker.block;

import alluxio.heartbeat.HeartbeatExecutor;

import com.google.common.base.Preconditions;

import java.util.Map;

public class BlockSpaceUpdater implements HeartbeatExecutor {

  private BlockWorker mBlockWorker;

  public BlockSpaceUpdater(BlockWorker blockWorker){
    mBlockWorker = Preconditions.checkNotNull(blockWorker, "blockworker");
  }

  @Override
  public void heartbeat() throws InterruptedException {
    Map<Long, BlockAccessRecord> blockAccessRecordMap = mBlockWorker.getBlockAccessRecords();
    for (Map.Entry<Long, BlockAccessRecord> entry : blockAccessRecordMap.entrySet()) {
      long blockId = entry.getKey();
      BlockAccessRecord record = entry.getValue();
      long totalAccessCount = record.getTotalAccessCount();
      long blockSize = record.getBlockSize();
      for (Map.Entry<Long, Integer> useAccessEntry : record.getUserIdToAccessCountMap().entrySet()) {
        long userId = useAccessEntry.getKey();
        long accessCount = useAccessEntry.getValue();
        mBlockWorker.updateBlockSpace(userId, blockId, accessCount * blockSize / totalAccessCount);
      }
    }
  }

  @Override
  public void close() {

  }
}
