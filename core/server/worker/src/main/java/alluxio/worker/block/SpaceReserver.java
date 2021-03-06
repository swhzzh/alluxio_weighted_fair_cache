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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatExecutor;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link SpaceReserver} periodically checks the available space on each storage tier. If the
 * used space is above the high watermark configured for the tier, eviction will be triggered to
 * reach the low watermark. If this is not the top tier, it will also add the amount of space
 * reserved on the tier above to reduce the impact of cascading eviction.
 */
@NotThreadSafe
public class SpaceReserver implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceReserver.class);

  /** The block worker the space reserver monitors. */
  private final BlockWorker mBlockWorker;

  /** Association between storage tier aliases and ordinals for the worker. */
  private final StorageTierAssoc mStorageTierAssoc;

  /** Mapping from tier alias to high watermark in bytes. */
  private final Map<String, Long> mHighWatermarks = new HashMap<>();

  /** Mapping from tier alias to space in bytes to be reserved on the tier. */
  private final Map<String, Long> mReservedSpaces = new HashMap<>();

  /**
   * Creates a new instance of {@link SpaceReserver}.
   *
   * @param blockWorker the block worker handle
   */
  public SpaceReserver(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    updateStorageInfo();
  }

  /**
   * Re-calculates storage spaces and watermarks.
   */
  public synchronized void updateStorageInfo() {
    Map<String, Long> tierCapacities = mBlockWorker.getStoreMeta().getCapacityBytesOnTiers();
    long lastTierReservedBytes = 0;
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal++) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long tierCapacity = tierCapacities.get(tierAlias);
      // High watermark defines when to start the space reserving process
      PropertyKey tierHighWatermarkProp =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(ordinal);
      double tierHighWatermarkConf = ServerConfiguration.getDouble(tierHighWatermarkProp);
      Preconditions.checkArgument(tierHighWatermarkConf > 0,
          "The high watermark of tier %s should be positive, but is %s", Integer.toString(ordinal),
          tierHighWatermarkConf);
      Preconditions.checkArgument(tierHighWatermarkConf < 1,
          "The high watermark of tier %s should be less than 1.0, but is %s",
          Integer.toString(ordinal), tierHighWatermarkConf);
      long highWatermark = (long) (tierCapacity * tierHighWatermarkConf);
      mHighWatermarks.put(tierAlias, highWatermark);

      // Low watermark defines when to stop the space reserving process if started
      PropertyKey tierLowWatermarkProp =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO.format(ordinal);
      double tierLowWatermarkConf = ServerConfiguration.getDouble(tierLowWatermarkProp);
      Preconditions.checkArgument(tierLowWatermarkConf >= 0,
          "The low watermark of tier %s should not be negative, but is %s",
          Integer.toString(ordinal), tierLowWatermarkConf);
      Preconditions.checkArgument(tierLowWatermarkConf < tierHighWatermarkConf,
          "The low watermark (%s) of tier %d should not be smaller than the high watermark (%s)",
          tierLowWatermarkConf, ordinal, tierHighWatermarkConf);
      long reservedSpace = (long) (tierCapacity - tierCapacity * tierLowWatermarkConf);
      lastTierReservedBytes += reservedSpace;
      // On each tier, we reserve no more than its capacity
      lastTierReservedBytes =
          (lastTierReservedBytes <= tierCapacity) ? lastTierReservedBytes : tierCapacity;
      mReservedSpaces.put(tierAlias, lastTierReservedBytes);
    }
  }

  private synchronized void reserveSpace() {
    Map<String, Long> usedBytesOnTiers = mBlockWorker.getStoreMeta().getUsedBytesOnTiers();
    for (int ordinal = mStorageTierAssoc.size() - 1; ordinal >= 0; ordinal--) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long reservedSpace = mReservedSpaces.get(tierAlias);
      if (mHighWatermarks.containsKey(tierAlias)) {
        long highWatermark = mHighWatermarks.get(tierAlias);
        if (usedBytesOnTiers.get(tierAlias) >= highWatermark) {
          try {
            mBlockWorker.freeUserSpace(Sessions.MIGRATE_DATA_SESSION_ID, -1L, reservedSpace, tierAlias);
          } catch (WorkerOutOfSpaceException | BlockDoesNotExistException
              | BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
            LOG.warn("SpaceReserver failed to free {} bytes on tier {} for high watermarks: {}",
                reservedSpace, tierAlias, e.getMessage());
          }
        }
      } else {
        LOG.warn("No watermark set for tier {}, eviction will not be run.", tierAlias);
      }
    }
  }

  @Override
  public void heartbeat() {
    reserveSpace();
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
