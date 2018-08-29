package alluxio.web;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

public class TarGZIPUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TarGZIPUtils.class);

    /**
     * @param  tarName target tar name
     * @param folders directory
     */
    public static void createTarFile(String tarName,Function<String, Boolean> filter,String... folders){
        TarArchiveOutputStream tarOs = null;
        try {
            FileOutputStream fos = new FileOutputStream(tarName);
            GZIPOutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos));
            // Using input name to create output name
            tarOs = new TarArchiveOutputStream(gos);
            for(String folder : folders) {
                File dir = new File(folder);
                addFilesToTarGZ(new File(folder).getName() ,dir, tarOs,filter);
            }
        } catch (IOException e) {
            LOG.error("create tar {} failed {}",tarName,e);
        }finally{
            try {
                tarOs.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

 /**
  * 
  * @param source directory
  */
 public static void createTarFile(String tarName,Function<String, Boolean> filter,String source){
  TarArchiveOutputStream tarOs = null;
  try {
    // Using input name to create output name
    FileOutputStream fos = new FileOutputStream(tarName.concat(".tar.gz"));
    GZIPOutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos));
    tarOs = new TarArchiveOutputStream(gos);
    File folder = new File(source);
    File[] fileNames = folder.listFiles();
    for(File file : fileNames){
      addFilesToTarGZ(folder.getName(),file, tarOs,filter);
    }
  } catch (IOException e) {
      LOG.error("create tar {} failed {}",tarName,e);
  }finally{
    try {
     tarOs.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
 }



 /**
  * 
  * @param file file or directory
  * @param tos
  * @throws IOException
  */
 public static void addFilesToTarGZ(String parentName , File file, TarArchiveOutputStream tos , Function<String, Boolean> filter)
                   throws IOException{
     if(!filter.apply(file.getName())) {
        return;
     }

   // New TarArchiveEntry
   tos.putArchiveEntry(new TarArchiveEntry(file,parentName + File.separator + file.getName()));
//   filter files like jar
   if(file.isFile()){
       FileInputStream fis = new FileInputStream(file);
       BufferedInputStream bis = new BufferedInputStream(fis);
       // Write content of the file
       IOUtils.copy(bis, tos);
       tos.closeArchiveEntry();
       fis.close();
   }else if(file.isDirectory()){
     // no need to copy any content since it is a directory, just close the outputstream
     tos.closeArchiveEntry();
     for(File cFile : file.listFiles()){
       // recursively call the method for all the subfolders
       addFilesToTarGZ(parentName + File.separator + file.getName(), cFile, tos,filter);
     }
   }
  
 }
}