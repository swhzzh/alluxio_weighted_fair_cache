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

package alluxio.worker.block.evictor;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.UserStorageInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is used to evict blocks by LRFU. LRFU evict blocks with minimum CRF, where CRF of a
 * block is the sum of F(t) = pow(1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor}).
 * Each access to a block has a F(t) value and t is the time interval since that access to current.
 * As the formula of F(t) shows, when (1.0 / {@link #mStepFactor}) time units passed, F(t) will
 * cut to the (1.0 / {@link #mAttenuationFactor}) of the old value. So {@link #mStepFactor}
 * controls the step and {@link #mAttenuationFactor} controls the attenuation. Actually, LRFU
 * combines LRU and LFU, it evicts blocks with small frequency or large recency. When
 * {@link #mStepFactor} is close to 0, LRFU is close to LFU. Conversely, LRFU is close to LRU
 * when {@link #mStepFactor} is close to 1.
 */
@NotThreadSafe
public final class LRFUEvictor extends AbstractEvictor {

  private static final Logger LOG = LoggerFactory.getLogger(LRFUEvictor.class);
  /** Map from block id to the last updated logic time count. */
  private final Map<Long, Long> mBlockIdToLastUpdateTime = new ConcurrentHashMap<>();
  // Map from block id to the CRF value of the block
  private final Map<Long, Double> mBlockIdToCRFValue = new ConcurrentHashMap<>();
  /** In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU. */
  private final double mStepFactor;
  /** The attenuation factor is in the range of [2, INF]. */
  private final double mAttenuationFactor;

  // for multi user env
  private final Map<Long, Map<Long, Long>> mUserIdToBlockIdAndLastUpdateTimeMap = new ConcurrentHashMap<>();
  private final Map<Long, Map<Long, Double>> mUserIdToBlockIdAndCRFValueMap = new ConcurrentHashMap<>();
  protected Map<Long, Long> mUserIdToMarkOutSize = new HashMap<>();

  /** Logic time count. */
  private AtomicLong mLogicTimeCount = new AtomicLong(0L);

  /**
   * Creates a new instance of {@link LRFUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LRFUEvictor(BlockMetadataEvictorView view, Allocator allocator) {
    super(view, allocator);
    mStepFactor = ServerConfiguration.getDouble(PropertyKey.WORKER_EVICTOR_LRFU_STEP_FACTOR);
    mAttenuationFactor =
        ServerConfiguration.getDouble(PropertyKey.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR);
    Preconditions.checkArgument(mStepFactor >= 0.0 && mStepFactor <= 1.0,
        "Step factor should be in the range of [0.0, 1.0]");
    Preconditions.checkArgument(mAttenuationFactor >= 2.0,
        "Attenuation factor should be no less than 2.0");

    // Preloading blocks
    /*for (StorageTierView tier : mMetadataView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        for (BlockMeta block : ((StorageDirEvictorView) dir).getEvictableBlocks()) {
          mBlockIdToLastUpdateTime.put(block.getBlockId(), 0L);
          mBlockIdToCRFValue.put(block.getBlockId(), 0.0);
        }
      }
    }*/
  }

  /**
   * Calculates weight of an access, which is the function value of
   * F(t) = pow (1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor}).
   *
   * @param logicTimeInterval time interval since that access to current
   * @return Function value of F(t)
   */
  private double calculateAccessWeight(long logicTimeInterval) {
    return Math.pow(1.0 / mAttenuationFactor, logicTimeInterval * mStepFactor);
  }

  @Nullable
  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataEvictorView view, Mode mode) {
    mMetadataView = view;
    updateAllUserCRFValue();
    Map<Long, Map<Long, BlockStoreLocation>> userIdToEvict = new HashMap<>();
    EvictionPlan plan = new EvictionPlan(userIdToEvict);
    StorageDirEvictorView candidateDir = freeSpaceWithWeightedMaxMinFairPolicy(bytesToBeAvailable, location, plan, mode);

    mMetadataView.clearBlockMarks();
    mUserIdToMarkOutSize.clear();
    if (candidateDir == null) {
      return null;
    }

    return plan;
  }

  private StorageDirEvictorView freeSpaceWithWeightedMaxMinFairPolicy(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan, Mode mode){
    // 1. If bytesToBeAvailable can already be satisfied without eviction, return the eligible
    // StorageDirView
    StorageDirEvictorView candidateDirView = (StorageDirEvictorView)
        EvictorUtils.selectDirWithRequestedSpace(bytesToBeAvailable, location, mMetadataView);
    if (candidateDirView != null) {
      return candidateDirView;
    }

    // 2. Iterate over blocks in order until we find a StorageDirEvictorView that is
    // in the range of location and can satisfy bytesToBeAvailable
    // after evicting its blocks iterated so far
    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
    Map<Long, UserStorageInfo> userStorageInfoMap = mMetadataView.getAllUserStorageInfo();
    for (Long userId : userStorageInfoMap.keySet()) {
      mUserIdToMarkOutSize.put(userId, 0L);
    }
    Map<Long, Iterator<Long>> its = new HashMap<>();
    while (dirCandidates.candidateSize() < bytesToBeAvailable){
      long userId = getEvictUser(userStorageInfoMap);
      if (userId == -1L){
        return null;
      }
      Iterator<Long> it = its.get(userId);
      if (it == null){
        it = getUserBlockIterator(userId);
        its.put(userId, it);
      }
      if (it.hasNext()){
        long blockId = it.next();
        try {
          BlockMeta blockMeta = mMetadataView.getUserBlockMeta(userId, blockId);
          if (blockMeta != null){
            if (blockMeta.getBlockLocation().belongsTo(location)){
              String tierAlias = blockMeta.getParentDir().getParentTier().getTierAlias();
              int dirIndex = blockMeta.getParentDir().getDirIndex();
              long spaceUsed = userStorageInfoMap.get(userId).getBlockUsedSpace(blockId);
              dirCandidates.add((StorageDirEvictorView) mMetadataView.getTierView(tierAlias)
                  .getDirView(dirIndex), userId, blockId, spaceUsed);
              mUserIdToMarkOutSize.put(userId, mUserIdToMarkOutSize.get(userId) + spaceUsed);
            }
          }
        } catch (BlockDoesNotExistException e) {
          LOG.warn("Remove block {} from user {} evictor cache because {}", blockId, userId, e);
          it.remove();
          onRemoveUserBlockFromIterator(userId, blockId);
        }
      }
    }

    // 3. If there is no eligible StorageDirEvictorView, return null
    if (mode == Mode.GUARANTEED && dirCandidates.candidateSize() < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to allocate space in the next tier to move candidate blocks
    // there. If allocation fails, the next tier will continue to evict its blocks to free space.
    // Blocks are only evicted from the last tier or it can not be moved to the next tier.
    candidateDirView = dirCandidates.candidateDir();
    if (candidateDirView == null) {
      return null;
    }
    List<Pair<Long, Long>> candidateUserBlocks = dirCandidates.candidateUserBlocks();
    for (Pair<Long, Long> candidateUserBlock : candidateUserBlocks) {
      long blockId = candidateUserBlock.getFirst();
      long userId = candidateUserBlock.getSecond();
      try {
        BlockMeta blockMeta = mMetadataView.getUserBlockMeta(userId, blockId);
        if (blockMeta != null){
          candidateDirView.markUserBlockMoveOut(userId, blockId, userStorageInfoMap.get(userId).getBlockUsedSpace(blockId));
          plan.toEvictMap().computeIfAbsent(userId, k -> new HashMap<>());
          plan.toEvictMap().get(userId).put(blockId, candidateDirView.toBlockStoreLocation());
        }
      } catch (BlockDoesNotExistException e) {
        continue;
      }
    }
    return candidateDirView;
  }


  private long getEvictUser(Map<Long, UserStorageInfo> userStorageInfoMap){
    double max = 0;
    long evictUser = -1;
    for (Map.Entry<Long, UserStorageInfo> entry : userStorageInfoMap.entrySet()) {
      long userId = entry.getKey();
      UserStorageInfo userStorageInfo = entry.getValue();
      double curW = (userStorageInfo.getUsedBytes() - mUserIdToMarkOutSize.get(userId)) /  userStorageInfo.getWeight();
      if ( curW > max){
        max = curW;
        evictUser = userId;
      }
    }
    return evictUser;
  }


  @Override
  protected Iterator<Long> getBlockIterator() {
    return Iterators.transform(getSortedCRF().iterator(), Entry::getKey);
  }

  @Override
  protected Iterator<Long> getUserBlockIterator(long userId) {
    return Iterators.transform(getUserSortedCRF(userId).iterator(), Entry::getKey);
  }

  /**
   * Sorts all blocks in ascending order of CRF.
   *
   * @return the sorted CRF of all blocks
   */
  private List<Map.Entry<Long, Double>> getSortedCRF() {
    List<Map.Entry<Long, Double>> sortedCRF = new ArrayList<>(mBlockIdToCRFValue.entrySet());
    sortedCRF.sort(Comparator.comparingDouble(Entry::getValue));
    return sortedCRF;
  }

  private List<Map.Entry<Long, Double>> getUserSortedCRF(long userId){
    Map<Long, Double> blockIdToCRFValue = mUserIdToBlockIdAndCRFValueMap.get(userId);
    if (blockIdToCRFValue != null) {
      List<Map.Entry<Long, Double>> sortedCRF = new ArrayList<>(blockIdToCRFValue.entrySet());
      sortedCRF.sort(Comparator.comparingDouble(Entry::getValue));
      return sortedCRF;
    }
    return null;
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    updateOnAccessAndCommit(blockId);
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    updateOnAccessAndCommit(blockId);
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    updateOnRemoveBlock(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    updateOnRemoveBlock(blockId);
  }

  @Override
  public void onBlockLost(long blockId) {
    updateOnRemoveBlock(blockId);
  }

  @Override
  protected void onRemoveBlockFromIterator(long blockId) {
    mBlockIdToLastUpdateTime.remove(blockId);
    mBlockIdToCRFValue.remove(blockId);
  }

  @Override
  protected void onRemoveUserBlockFromIterator(long userId, long blockId) {
    if (mUserIdToBlockIdAndCRFValueMap.containsKey(userId)){
      mUserIdToBlockIdAndCRFValueMap.get(userId).remove(blockId);
    }
    if (mUserIdToBlockIdAndLastUpdateTimeMap.containsKey(userId)){
      mUserIdToBlockIdAndLastUpdateTimeMap.get(userId).remove(blockId);
    }
  }

  @Override
  public void onUserAccessBlock(long sessionId, long userId, long blockId) {
    updateOnUserAccessAndCommitBlock(userId, blockId);
  }

  @Override
  public void onUserCommitBlock(long sessionId, long userId, long blockId,
      BlockStoreLocation location) {
    updateOnUserAccessAndCommitBlock(userId, blockId);
  }

  @Override
  public void onUserRemoveBlockByClient(long sessionId, long userId, long blockId) {
    updateOnUserRemoveBlock(userId, blockId);
  }

  @Override
  public void onUserRemoveBlockByWorker(long sessionId, long userId, long blockId) {
    updateOnUserRemoveBlock(userId, blockId);
  }

  @Override
  public void onUserBlockLost(long userId, long blockId) {
    updateOnUserRemoveBlock(userId, blockId);
  }

  /**
   * This function is used to update CRF of all the blocks according to current logic time. When
   * some block is accessed in some time, only CRF of that block itself will be updated to current
   * time, other blocks who are not accessed recently will only be updated until
   * {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataEvictorView)} is called
   * because blocks need to be sorted in the increasing order of CRF. When this function is called,
   * {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} need to be locked in case
   * of the changing of values.
   */
  private void updateCRFValue() {
    long currentLogicTime = mLogicTimeCount.get();
    for (Entry<Long, Double> entry : mBlockIdToCRFValue.entrySet()) {
      long blockId = entry.getKey();
      double crfValue = entry.getValue();
      mBlockIdToCRFValue.put(blockId, crfValue
          * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId)));
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  // why??
  private void updateUserCRFValue(long userId){
    long currentLogicTime = mLogicTimeCount.get();
    Map<Long, Double> blockIdToCRFValue = mUserIdToBlockIdAndCRFValueMap.get(userId);
    Map<Long, Long> blockIdToLastUpdateTime = mUserIdToBlockIdAndLastUpdateTimeMap.get(userId);
    if (blockIdToCRFValue != null && blockIdToLastUpdateTime != null) {
      for (Entry<Long, Double> entry : blockIdToCRFValue.entrySet()) {
        long blockId = entry.getKey();
        double crfValue = entry.getValue();
        blockIdToCRFValue.put(blockId, crfValue
            * calculateAccessWeight(currentLogicTime - blockIdToLastUpdateTime.get(blockId)));
        blockIdToLastUpdateTime.put(blockId, currentLogicTime);
      }
    }
  }


  private void updateAllUserCRFValue(){
    long currentLogicTime = mLogicTimeCount.get();
    for (Entry<Long, Map<Long, Double>> entry : mUserIdToBlockIdAndCRFValueMap.entrySet()) {
      long userId = entry.getKey();
      Map<Long, Double> blockIdToCRFValue = entry.getValue();
      Map<Long, Long> blockIdToLastUpdateTime = mUserIdToBlockIdAndLastUpdateTimeMap.get(userId);
      if (blockIdToCRFValue != null && blockIdToLastUpdateTime != null) {
        for (Entry<Long, Double> subEntry : blockIdToCRFValue.entrySet()) {
          long blockId = subEntry.getKey();
          double crfValue = subEntry.getValue();
          blockIdToCRFValue.put(blockId, crfValue
              * calculateAccessWeight(currentLogicTime - blockIdToLastUpdateTime.get(blockId)));
          blockIdToLastUpdateTime.put(blockId, currentLogicTime);
        }
      }
    }
  }

  /**
   * Updates {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} when block is
   * accessed or committed. Only CRF of the accessed or committed block will be updated, CRF
   * of other blocks will be lazily updated (only when {@link #updateCRFValue()} is called).
   * If the block is updated at the first time, CRF of the block will be set to 1.0, otherwise
   * the CRF of the block will be set to {1.0 + old CRF * F(current time - last update time)}.
   *
   * @param blockId id of the block to be accessed or committed
   */
  private void updateOnAccessAndCommit(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      long currentLogicTime = mLogicTimeCount.incrementAndGet();
      // update CRF value
      // CRF(currentLogicTime)=CRF(lastUpdateTime)*F(currentLogicTime-lastUpdateTime)+F(0)
      if (mBlockIdToCRFValue.containsKey(blockId)) {
        mBlockIdToCRFValue.put(blockId, mBlockIdToCRFValue.get(blockId)
            * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId))
            + 1.0);
      } else {
        mBlockIdToCRFValue.put(blockId, 1.0);
      }
      // update currentLogicTime to lastUpdateTime
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  private void updateOnUserAccessAndCommitBlock(long userId, long blockId){
    long currentLogicTime = mLogicTimeCount.incrementAndGet();
    Map<Long, Double> blockIdToCRFValue = mUserIdToBlockIdAndCRFValueMap.get(userId);
    Map<Long, Long> blockIdToLastUpdateTime = mUserIdToBlockIdAndLastUpdateTimeMap.get(userId);
    if (blockIdToCRFValue == null){
      blockIdToCRFValue = new ConcurrentHashMap<>();
      mUserIdToBlockIdAndCRFValueMap.put(userId, blockIdToCRFValue);
    }
    if (blockIdToLastUpdateTime == null){
      blockIdToLastUpdateTime = new ConcurrentHashMap<>();
      mUserIdToBlockIdAndLastUpdateTimeMap.put(userId, blockIdToLastUpdateTime);
    }
    if (blockIdToCRFValue.containsKey(blockId)){
      blockIdToCRFValue.put(blockId, blockIdToCRFValue.get(blockId)
          * calculateAccessWeight(currentLogicTime - blockIdToLastUpdateTime.get(blockId))
          + 1.0);
    }else {
      blockIdToCRFValue.put(blockId, 1.0);
    }
    blockIdToLastUpdateTime.put(blockId, currentLogicTime);
  }

  /**
   * Updates {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} when block is
   * removed.
   *
   * @param blockId id of the block to be removed
   */
  private void updateOnRemoveBlock(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      mLogicTimeCount.incrementAndGet();
      mBlockIdToCRFValue.remove(blockId);
      mBlockIdToLastUpdateTime.remove(blockId);
    }
  }

  private void updateOnUserRemoveBlock(long userId, long blockId){
    mLogicTimeCount.incrementAndGet();
    Map<Long, Double> blockIdToCRFValue = mUserIdToBlockIdAndCRFValueMap.get(userId);
    if (blockIdToCRFValue != null){
      blockIdToCRFValue.remove(blockId);
    }
    Map<Long, Long> blockIdToLastUpdateTime = mUserIdToBlockIdAndLastUpdateTimeMap.get(userId);
    if (blockIdToLastUpdateTime != null){
      blockIdToLastUpdateTime.remove(blockId);
    }
  }
}
