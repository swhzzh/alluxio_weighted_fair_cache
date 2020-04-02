package alluxio.worker.block;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockAccessRecord {
  private long mBlockId;
  private long mBlockSize;
  private Map<Long, Integer> mUserIdToAccessCountMap = new ConcurrentHashMap<>();
  private AtomicInteger mTotalAccessCount = new AtomicInteger(0);

  public BlockAccessRecord(long blockId, long blockSize){
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  public void accessBlock(long userId){
    mUserIdToAccessCountMap.put(userId, mUserIdToAccessCountMap.getOrDefault(userId, 0) + 1 );
    mTotalAccessCount.incrementAndGet();
  }

  public boolean removeUser(long userId){
    if (mUserIdToAccessCountMap.containsKey(userId)){
      int count = mUserIdToAccessCountMap.get(userId);
      mUserIdToAccessCountMap.remove(userId);
      if (mUserIdToAccessCountMap.isEmpty()){
        return true;
      }
      mTotalAccessCount.addAndGet(-count);
    }
    return false;
  }

  public int getUserCount(){
    return mUserIdToAccessCountMap.size();
  }

  public Map<Long, Integer> getUserIdToAccessCountMap() {
    return mUserIdToAccessCountMap;
  }

  public int getTotalAccessCount() {
    return mTotalAccessCount.get();
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getBlockSize() {
    return mBlockSize;
  }
}
