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

import alluxio.Sessions;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;
import alluxio.worker.block.meta.UserStorageInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of an evictor which follows the least recently used algorithm. It discards the
 * least recently used item based on its access.
 */
@NotThreadSafe
public class LRUEvictor extends AbstractEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(LRUEvictor.class);



  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  /**
   * Access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  protected Map<Long, Boolean> mLRUCache =
      Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));
  protected Map<Long, Map<Long, Boolean>> mUserIdToLRUCacheMap = new HashMap<>();
  protected Map<Long, Long> mUserIdToMarkOutSize = new HashMap<>();
  /**
   * Creates a new instance of {@link LRUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LRUEvictor(BlockMetadataEvictorView view, Allocator allocator) {
    super(view, allocator);

    // preload existing blocks loaded by StorageDir to Evictor
    for (StorageTierView tierView : mMetadataView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        for (BlockMeta blockMeta : ((StorageDirEvictorView) dirView)
            .getEvictableBlocks()) { // all blocks with initial view
          mLRUCache.put(blockMeta.getBlockId(), UNUSED_MAP_VALUE);
        }
      }
    }
    for (Long userId : mMetadataView.getAllUsers()) {
      mUserIdToLRUCacheMap.put(userId, Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED)));
      /*Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          for (BlockMeta blockMeta : ((StorageDirEvictorView) dirView)
              .getUserEvictableBlocks(userId)) {
            map.put(blockMeta.getBlockId(), UNUSED_MAP_VALUE);
          }
        }
      }*/
    }
  }


  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataEvictorView view) {
    return freeSpaceWithView(bytesToBeAvailable, location, view, Mode.BEST_EFFORT);
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataEvictorView view, Mode mode) {
    mMetadataView = view;

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
    List<Long> blocks = new ArrayList<>(mLRUCache.keySet());
    return blocks.iterator();
  }

  @Override
  protected Iterator<Long> getUserBlockIterator(long userId) {
    if (!mUserIdToLRUCacheMap.containsKey(userId)){
      return null;
    }
    List<Long> blocks = new ArrayList<>(mUserIdToLRUCacheMap.get(userId).keySet());
    return blocks.iterator();
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onUserAccessBlock(long sessionId, long userId, long blockId) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
    // Since the temp block has been committed, update Evictor about the new added blocks
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onUserCommitBlock(long sessionId, long userId, long blockId,
      BlockStoreLocation location) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onUserMoveBlockByClient(long sessionId, long userId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.remove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onUserMoveBlockByWorker(long sessionId, long userId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.remove(blockId);
  }

  @Override
  public void onBlockLost(long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onUserBlockLost(long userId, long blockId) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.remove(blockId);
  }

  @Override
  protected void onRemoveBlockFromIterator(long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  protected void onRemoveUserBlockFromIterator(long userId, long blockId) {
    Map<Long, Boolean> map = mUserIdToLRUCacheMap.get(userId);
    if (map == null){
      return;
    }
    map.remove(blockId);
  }
}
