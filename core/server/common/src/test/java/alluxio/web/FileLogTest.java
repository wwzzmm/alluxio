package alluxio.web;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.UIFileInfo;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FileLogTest {
    @Test
    public void testListSparkLog() {
        FilenameFilter LOG_FILE_FILTER = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().contains("std");
            }
        };
        List<UIFileInfo> fileInfos = new ArrayList<>();
        String logsPath = "/home/wang/Downloads/work";
        File logsDir = new File(logsPath);
        File[] sparkLogFiles = logsDir.listFiles();
        if (sparkLogFiles != null) {
            for (File logFile : sparkLogFiles) {
                if (logFile.isDirectory()) {
                    File[] innerLogs = logFile.listFiles(LOG_FILE_FILTER);
                    for (File iLogFile : innerLogs) {
                        String logFileName = logFile.getName() + "/" + iLogFile.getName();
                        fileInfos.add(new UIFileInfo(new UIFileInfo.LocalFileInfo(logFileName, logFileName,
                                iLogFile.length(), UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME,
                                iLogFile.lastModified(), iLogFile.isDirectory())));
                    }
                }else{
                    String logFileName = logFile.getName();
                    fileInfos.add(new UIFileInfo(new UIFileInfo.LocalFileInfo(logFileName, logFileName,
                            logFile.length(), UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME,
                            logFile.lastModified(), logFile.isDirectory())));
                }
            }
        }
        fileInfos.stream().forEach(e -> System.out.println(e.getName()));
    }

    @Test
    public void getFileContent(){
        String logsPath = "/home/wang/Downloads/work";
        File logsDir = new File(logsPath,"app1/stdout");
        System.out.println(logsDir.getAbsolutePath()+" "+logsDir.length());
    }
    @Test
    public void getFilePath(){
        String logsPath = "/home/wang/Downloads/work/app1";
        File logsDir = new File(logsPath);
        File[] sparkLogFiles = logsDir.listFiles();
        if (sparkLogFiles != null) {
            for (File logFile : sparkLogFiles) {
                System.out.println(logFile.getPath()+" "+logFile.getName());
            }
        }
    }

    @Test
    public void testTar(){
        String tmpTarPath = "/home/wang/Downloads/all-logs";
        Function<String, Boolean> filter = s -> !s.contains("jar")&& !s.contains("tar.gz");
        String logsPath = Configuration.get(PropertyKey.LOGS_DIR);
        String hdfsLogsPath = Configuration.get(PropertyKey.HDFS_LOGS_DIR);
        String sparkLogsPath = "/home/wang/Downloads/work";//Configuration.get(PropertyKey.SPARK_LOGS_DIR);
        TarGZIPUtils.createTarFile(tmpTarPath.concat(NetworkAddressUtils.getLocalHostMetricName()),filter,sparkLogsPath);

    }

}
