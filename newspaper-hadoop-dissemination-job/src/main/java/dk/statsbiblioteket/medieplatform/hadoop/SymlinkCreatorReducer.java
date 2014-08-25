package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SymlinkCreatorReducer extends AbstractDomsReducer {

    public static final String SYMLINK_ROOTDIR_PATH = "symlink.rootdir.path";
    public static final String SYMLINK_DEPTH = "symlink.depth";
    private static Logger log = LoggerFactory.getLogger(SymlinkCreatorReducer.class);

    protected void reduceJava6(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        log.debug("Doing reduce ...");
        try {
            String pid = getDomsPid(key);
            String outputPathString = values.iterator().next().toString();
            log.debug("Creating a symlink for {} to {}.", pid, outputPathString);
            String filename = pid.replace("uuid:","") + ".jp2";
            File symlinkRootDir = new File(context.getConfiguration().get(SYMLINK_ROOTDIR_PATH));
            String symlinkPath = "";
            int symlinkDepth = context.getConfiguration().getInt(SYMLINK_DEPTH, 0);
            for (int i = 0; i < symlinkDepth; i++) {
                symlinkPath += filename.charAt(i) + "/";
            }
            File symlinkDir = null;
            if (symlinkDepth > 0) {
               symlinkDir = new File(symlinkRootDir, symlinkPath);
            } else {
                symlinkDir = symlinkRootDir;
            }
            log.debug("Creating directory {} (if necessary).", symlinkDir.getAbsolutePath());
            symlinkDir.mkdirs();
            File symlinkFile = new File(symlinkDir, filename);
            Runtime rt = Runtime.getRuntime();
            String symlinkFileAbsolutePath = symlinkFile.getAbsolutePath();
            log.debug("Creating symbolic link from {} to {}.", symlinkFileAbsolutePath, outputPathString);
            Process proc = rt.exec(new String[]{"ln", "-s", outputPathString, symlinkFileAbsolutePath});
            proc.waitFor();
            context.write(key, new Text(symlinkFileAbsolutePath));
        } catch (Exception e) {
            log.error("Error in reduce for " + key.toString() +"", e);
            throw new IOException(e);
        }
    }

    protected void reduceJava7(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        log.debug("Doing reduce ...");
        try {
            String pid = getDomsPid(key);
            String outputPathString = values.iterator().next().toString();
            log.debug("Creating a symlink for {} to {}.", pid, outputPathString);
            String filename = pid.replace("uuid:","") + ".jp2";
            String symlinkRoot = context.getConfiguration().get(SYMLINK_ROOTDIR_PATH);
            List<String> symlinkDirPathElements = new ArrayList<String>();
            int symlinkDepth = context.getConfiguration().getInt(SYMLINK_DEPTH, 0);
            for (int i = 0; i < symlinkDepth; i++) {
                symlinkDirPathElements.add(filename.charAt(i) + "");
            }
            Path symlinkDirPath = Paths.get(symlinkRoot, symlinkDirPathElements.toArray(new String[]{}));
            log.debug("Creating directory {} (if necessary).", symlinkDirPath.toString());
            Files.createDirectories(symlinkDirPath);
            List<String> symlinkFilePathElements = symlinkDirPathElements;
            symlinkFilePathElements.add(filename);
            Path symlinkFilePath = Paths.get(symlinkRoot, symlinkFilePathElements.toArray(new String[]{}));
            log.debug("Creating symlink {} for {}.", symlinkFilePath.toString(), outputPathString);
            Files.createSymbolicLink(symlinkFilePath, Paths.get(outputPathString));
            context.write(key, new Text(symlinkFilePath.toFile().getAbsolutePath()));
        } catch (Exception e) {
            log.error("Error in reduce for " + key.toString() +"", e);
            throw new IOException(e);
        }
    }

    /**
     * Creates a symlink from the doms-pid to the dissemination file.
     *
     * This method uses native Java call for Java 7, and external (linux) commands for
     * Java 6, so the Java 7 method should be preferred.
     *
     * @param key The original file
     * @param values A single element, the final output file
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (System.getProperty("java.version").startsWith("1.6.")) {
            reduceJava6(key, values, context);
        }  else {
            reduceJava7(key, values, context);
        }
    }
}
