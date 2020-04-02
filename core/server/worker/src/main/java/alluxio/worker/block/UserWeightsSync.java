package alluxio.worker.block;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.worker.file.FileSystemMasterClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UserWeightsSync implements HeartbeatExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(UserWeightsSync.class);

  private final BlockWorker mBlockWorker;
  private FileSystemMasterClient mFileSystemMasterClient;

  public UserWeightsSync(BlockWorker blockWorker, FileSystemMasterClient fileSystemMasterClient) {
    mBlockWorker = blockWorker;
    mFileSystemMasterClient = fileSystemMasterClient;
  }

  @Override
  public void heartbeat() throws InterruptedException {
    try {
      Map<Long, Double> userWeights =
          mFileSystemMasterClient.getUserWeights(mBlockWorker.getWorkerId().get());
      mBlockWorker.updateUserWeights(userWeights);
    } catch (AlluxioStatusException e) {
      LOG.warn("Failed to receive user weights: {}", e.getMessage());
      LOG.debug("Exception: ", e);
    }
  }

  @Override
  public void close() {

  }
}
