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

package alluxio.worker.block;

import alluxio.exception.BlockDoesNotExistException;
import alluxio.master.block.BlockId;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierEvictorView;
import alluxio.worker.block.meta.StorageTierView;
import alluxio.worker.block.meta.UserStorageInfo;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors,
 * filtering out un-evictable blocks and un-allocatable space internally, so that evictors and
 * allocators can be developed with much simpler logic, without worrying about various constraints,
 * e.g. pinned files, locked blocks, etc.
 *
 * TODO(cc): Filter un-allocatable space.
 */
@NotThreadSafe
public class BlockMetadataEvictorView extends BlockMetadataView {

  /** The {@link BlockMetadataManager} this view is derived from. */
  private final BlockMetadataManager mMetadataManager;

  /** A list of pinned inodes, including inodes which are scheduled for async persist. */
  private final Set<Long> mPinnedInodes = new HashSet<>();

  /** Indices of locks that are being used. */
  private final Set<Long> mInUseBlocks = new HashSet<>();
  private final Map<Long, Set<Long>> mUserIdToPinnedInodesMap = new HashMap<>();
  private final Map<Long, Set<Long>> mUserIdToInUserBlocksMap = new HashMap<>();
  /**
   * Creates a new instance of {@link BlockMetadataEvictorView}. Now we always create a new view
   * before freespace.
   *
   * @param manager which the view should be constructed from
   * @param pinnedInodes a set of pinned inodes
   * @param lockedBlocks a set of locked blocks
   */
  // TODO(qifan): Incrementally update the view.
  public BlockMetadataEvictorView(BlockMetadataManager manager, Set<Long> pinnedInodes,
      Set<Long> lockedBlocks) {
    super(manager);
    mMetadataManager = manager;
    mPinnedInodes.addAll(Preconditions.checkNotNull(pinnedInodes, "pinnedInodes"));
    Preconditions.checkNotNull(lockedBlocks, "lockedBlocks");
    mInUseBlocks.addAll(lockedBlocks);

    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : manager.getTiers()) {
      StorageTierEvictorView tierView = new StorageTierEvictorView(tier, this);
      mTierViews.add(tierView);
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
    }
  }

  public BlockMetadataEvictorView(BlockMetadataManager manager, Set<Long> pinnedInodes,
      Set<Long> lockedBlocks, Map<Long, Set<Long>> userIdToPinnedInodes, Map<Long, Set<Long>> userIdToInUserBlocks) {
    super(manager);
    mMetadataManager = manager;
    mPinnedInodes.addAll(Preconditions.checkNotNull(pinnedInodes, "pinnedInodes"));
    Preconditions.checkNotNull(lockedBlocks, "lockedBlocks");
    mInUseBlocks.addAll(lockedBlocks);
    mUserIdToPinnedInodesMap.putAll(userIdToPinnedInodes);
    mUserIdToInUserBlocksMap.putAll(userIdToInUserBlocks);

    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : manager.getTiers()) {
      StorageTierEvictorView tierView = new StorageTierEvictorView(tier, this);
      mTierViews.add(tierView);
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
    }
  }

  /**
   * Clears all marks of blocks to move in/out in all dir views.
   */
  public void clearBlockMarks() {
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        ((StorageDirEvictorView) dirView).clearBlockMarks();
      }
    }
  }

  /**
   * Tests if the block is pinned either explicitly or because it is scheduled for async persist.
   *
   * @param blockId to be tested
   * @return boolean, true if block is pinned
   */
  public boolean isBlockPinned(long blockId) {
    return mPinnedInodes.contains(BlockId.getFileId(blockId));
  }
  public boolean isUserBlockPinned(long userId, long blockId){
    if (!mUserIdToPinnedInodesMap.containsKey(userId)){
      return false;
    }
    return mUserIdToPinnedInodesMap.get(userId).contains(BlockId.getFileId(blockId));
  }
  /**
   * Tests if the block is locked.
   *
   * @param blockId to be tested
   * @return boolean, true if block is locked
   */
  public boolean isBlockLocked(long blockId) {
    return mInUseBlocks.contains(blockId);
  }
  public boolean isUserBlockLocked(long userId, long blockId){
    if (!mUserIdToInUserBlocksMap.containsKey(userId)){
      return false;
    }
    return mUserIdToInUserBlocksMap.get(userId).contains(blockId);
  }
  /**
   * Tests if the block is evictable.
   *
   * @param blockId to be tested
   * @return boolean, true if the block can be evicted
   */
  public boolean isBlockEvictable(long blockId) {
    return (!isBlockPinned(blockId) && !isBlockLocked(blockId) && !isBlockMarked(blockId));
  }
  public boolean isUserBlockEvictable(long userId, long blockId){
    return !isUserBlockPinned(userId, blockId) && !isUserBlockLocked(userId, blockId)
        && !isUserBlockMarked(userId, blockId);
  }
  /**
   * Tests if the block is marked to move out of its current dir in this view.
   *
   * @param blockId the id of the block
   * @return boolean, true if the block is marked to move out
   */
  public boolean isBlockMarked(long blockId) {
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (((StorageDirEvictorView) dirView).isMarkedToMoveOut(blockId)) {
          return true;
        }
      }
    }
    return false;
  }
  public boolean isUserBlockMarked(long userId, long blockId){
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (((StorageDirEvictorView) dirView).isUserBlockMarkedToMoveOut(userId, blockId)){
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets available bytes given certain location
   * {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)}. Throws an
   * {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public long getAvailableBytes(BlockStoreLocation location) {
    return mMetadataManager.getAvailableBytes(location);
  }

  /**
   * Returns null if block is pinned or currently being locked, otherwise returns
   * {@link BlockMetadataManager#getBlockMeta(long)}.
   *
   * @param blockId the block id
   * @return metadata of the block or null
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this block id is found
   */
  @Nullable
  public BlockMeta getBlockMeta(long blockId) throws BlockDoesNotExistException {
    if (isBlockEvictable(blockId)) {
      return mMetadataManager.getBlockMeta(blockId);
    } else {
      return null;
    }
  }

  @Nullable
  public BlockMeta getUserBlockMeta(long userId, long blockId) throws BlockDoesNotExistException {
    if (isUserBlockEvictable(userId, blockId) && mMetadataManager.hasBlockMeta(userId, blockId)) {
      return mMetadataManager.getBlockMeta(blockId);
    } else {
      return null;
    }
  }

  public List<Long> getAllUsers(){
    return mMetadataManager.getAllUsers();
  }

  public Map<Long, UserStorageInfo> getAllUserStorageInfo(){
    return mMetadataManager.getAllUserStorageInfo();
  }
}
