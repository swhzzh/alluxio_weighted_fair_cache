package alluxio.worker.block.meta;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class UserStorageInfo {
  private long mUserId;
  private double mWeight;
  private AtomicLong mUsedBytes;
  private Map<Long, Long> mBlockIdToUsedSpaceMap;
  private List<Long> mTempBlockIds;

  public UserStorageInfo(long userId, double weight){
    mUserId = userId;
    mWeight = weight;
    mUsedBytes = new AtomicLong(0L);
    mBlockIdToUsedSpaceMap = new ConcurrentHashMap<>();
    mTempBlockIds = new ArrayList<>();
  }
  public double getWeight(){
    return mWeight;
  }

  public void setWeight(double weight){
    mWeight = weight;
  }
  public long getUsedBytes(){
    return mUsedBytes.get();
  }

  public List<Long> getBlockIds(){
    return new ArrayList<>(mBlockIdToUsedSpaceMap.keySet());
  }
  public List<Long> getTempBlockIds(){
    return mTempBlockIds;
  }
  public void addTempBlock(long blockId, long blockSize){
    if (!mTempBlockIds.contains(blockId)){
      mTempBlockIds.add(blockId);
      mUsedBytes.addAndGet(blockSize);
    }
  }
  public void removeTempBlock(long blockId, long blockSize){
    if (mTempBlockIds.contains(blockId)){
      mTempBlockIds.remove(blockId);
      mUsedBytes.addAndGet(-blockSize);
    }
  }

  public void addBlock(long blockId, long blockSize){
    if (!mBlockIdToUsedSpaceMap.containsKey(blockId)){
      mBlockIdToUsedSpaceMap.put(blockId, blockSize);
      mUsedBytes.addAndGet(blockSize);
    }
  }
  public void removeBlock(long blockId){
    if (mBlockIdToUsedSpaceMap.containsKey(blockId)){
      long spaceUsed = mBlockIdToUsedSpaceMap.get(blockId);
      mBlockIdToUsedSpaceMap.remove(blockId);
      mUsedBytes.addAndGet(-spaceUsed);
    }
  }

  public boolean containsBlock(long blockId){
    return mBlockIdToUsedSpaceMap.containsKey(blockId);
  }
  public boolean containsTempBlock(long blockId){
    return mTempBlockIds.contains(blockId);
  }

  public void updateBlockSpace(long blockId, long newSpaceUsed){
    if (mBlockIdToUsedSpaceMap.containsKey(blockId)){
      long oldSpace = mBlockIdToUsedSpaceMap.get(blockId);
      mBlockIdToUsedSpaceMap.put(blockId, newSpaceUsed);
      mUsedBytes.addAndGet(newSpaceUsed - oldSpace);
    }
  }

  public void updateUsedBytes(long newUsedBytes){
    mUsedBytes.addAndGet(newUsedBytes);
  }

  public long getBlockUsedSpace(long blockId){
    if (mBlockIdToUsedSpaceMap.containsKey(blockId)){
      return mBlockIdToUsedSpaceMap.get(blockId);
    }
    return -1;
  }

  public Map<Long, Long> getBlockIdToUsedSpaceMap(){
    return mBlockIdToUsedSpaceMap;
  }
}
