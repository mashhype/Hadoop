package bdpuh.hw2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Mashhood Syed
 */
public class ParallelLocalToHdfsCopy {
    
    public static void main(String [] args) throws IOException {
        
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://localhost:9000/user/hdadmin"), configuration);
        
        String sourcePath = args[0];
        String destPath = args[1];
        String threadCount = args[2];
        int threads = Integer.parseInt(threadCount);
        Path source = new Path(sourcePath);
        Path dest = new Path(destPath);
        
        LocalFileSystem localfs = FileSystem.getLocal(configuration);      
        File localCheck = new File(sourcePath);

        boolean destExists = true;
        destExists = fileSystem.isDirectory(dest);
        
        if (!localCheck.isDirectory()) {
            System.out.println("Your source dir. does not exist.  Create and try again.");
            System.exit(0);
        }
        else if (destExists) {
            System.out.println("Your dest. dir. exists.  Delete and try again.");
            System.exit(0);
        }
        else {
            System.out.println("\nYou entered: {" + sourcePath + "} for the source.\n{" + destPath +
                    "} for the destination\n" + "and {" + threadCount + "} for the number of threads.\n");
            boolean destDir = false;
            try {
                //make the dest. directory
                destDir = fileSystem.mkdirs(dest);
                FileStatus[] filesAtSource = null;
                filesAtSource = localfs.listStatus(source);
                
                ExecutorService executor = Executors.newFixedThreadPool(threads);
                
                //copy files from source to destination
                for (int i = 1; i < filesAtSource.length; i++) {
                    
                    Runnable worker = new WorkerThread();
                    executor.execute(worker);
                    fileSystem.copyFromLocalFile(filesAtSource[i].getPath(), dest);
                    
                }
                executor.shutdown();
                while (!executor.isTerminated()) { 
                
                }
                System.out.println("\nCopying from " + source + " to " + dest + " is complete.");
                //put the destination path items in an array for compression
                Path[] filesAtDest = null;
                filesAtDest = FileUtil.stat2Paths(fileSystem.listStatus(dest));
                Path pathToKeep = null;
                boolean pathToDelete = false;
                //open filestreams and compression stream
                FSDataInputStream fSDataInputStream = null;
                FSDataOutputStream fSDataOutputStream = null;
                CompressionCodec compressionCodec = new GzipCodec();
                CompressionOutputStream compressedOutputStream = null;
      
                for (Path path : filesAtDest) {
                    fSDataInputStream = fileSystem.open(path);
                    pathToKeep = new Path(path + ".gz");
                    fSDataOutputStream = fileSystem.create(pathToKeep);
                    compressedOutputStream = compressionCodec.createOutputStream(fSDataOutputStream);
                    IOUtils.copyBytes(fSDataInputStream, compressedOutputStream, configuration);
                    pathToDelete = fileSystem.delete(path);
                }
                System.out.println("\nCompression of files at " + dest + " is complete.\n");
                //close stuff you opened
                fSDataInputStream.close();
                fSDataOutputStream.close();
                compressedOutputStream.close();
                fileSystem.close();
                
            } catch (IOException ex) {
                Logger.getLogger(ParallelLocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
            } 

        }
    }
}
        
        
    

