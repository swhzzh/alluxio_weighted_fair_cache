package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.util.ConfigurationUtils;

import com.google.common.io.Closer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ReadTest {

  private static final String mUserDir = System.getProperty("user.dir");


  public static void main(String[] args) {
    Closer closer = Closer.create();
    FileWriter fileWriter = null;
    try {
      File file = new File(mUserDir + "/logs/user/user_read_test.log");
      if (!file.exists()){
        file.createNewFile();
      }
      fileWriter = closer.register(new FileWriter(file));
    }
    catch (IOException e){
      e.printStackTrace();
    }

    Map<String, FileSystem> mUserToFileSystemMap = new HashMap<>();
    for (int i = 1; i <= 4; i++) {
      InstancedConfiguration configuration = new InstancedConfiguration(ConfigurationUtils.defaults());
      configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "swh" + i);
      FileSystemContext fsContext =
          FileSystemContext.create(configuration);
      FileSystem fs =
          closer.register(FileSystem.Factory.create(fsContext));
      mUserToFileSystemMap.put("swh" + i, fs);
    }
    for (int i = 0; i < 50; i++) {
      String user = selectUserRandomly();
      String filePath = selectFileRandomly();
      AlluxioURI uri = new AlluxioURI(filePath);
      FileSystem fs = mUserToFileSystemMap.get(user);
      //if (!fs.exists(uri))
      System.out.println(i + "\t" + user + " read " + filePath);
      byte[] buf = new byte[Constants.MB];
      try (FileInStream is = fs.openFile(uri)) {
        int read = is.read(buf);
        while (read != -1) {
          //System.out.write(buf, 0, read);
          read = is.read(buf);
        }
      } catch (FileDoesNotExistException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (AlluxioException e) {
        e.printStackTrace();
      }
      try {
        Thread.sleep(5000);
        String userSpaceReport = fs.generateUserSpaceReport();
        fileWriter.append(String.valueOf(i)).append("\n").append(user).append(" read ").append(filePath).append("\n\n")
            .append("The user space report is :\n")
            .append(userSpaceReport).append("\n");
        if (i % 10 == 0){
          fileWriter.flush();
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }
    try {
      fileWriter.flush();
      Thread.sleep(5000);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    /*URIStatus status = fs.getStatus(path);

    if (status.isFolder()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }*/
    try {
      closer.close();
      /*for (FileSystem fileSystem : mUserToFileSystemMap.values()) {
        fileSystem.close();
      }*/
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String selectUserRandomly(){
    Random random = new Random();
    return "swh" + (random.nextInt(4) + 1);
  }

  private static String selectFileRandomly(){
    String baseDir = "/test_for_all/";
    Random random = new Random();
    int testNum = random.nextInt(4) + 1;
    int sizeNum = random.nextInt(5) + 1;
    return baseDir + "test" + testNum + "_" + sizeNum + "0M.txt";
  }
}
