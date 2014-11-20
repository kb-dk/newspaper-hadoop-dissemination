package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A hadoop reducer which creates a symlink to each of the newly created dissemination files.
 */
public class SymlinkCreatorReducer extends AbstractDomsReducer {

    public static final String SYMLINK_ROOTDIR_PATH = "symlink.rootdir.path";
    public static final String SYMLINK_DEPTH = "symlink.depth";
    private static Logger log = LoggerFactory.getLogger(SymlinkCreatorReducer.class);


    /**
     * Creates a symlink from the doms-pid to the dissemination file.
     *
     * This method uses native Java calls for Java 7, and external (linux) commands for
     * Java 6, so the Java 7 method should be preferred.
     *
     * @param key The original file.
     * @param values A single element, the final output file.
     * @param context The hadoop context for the job.
     * @throws IOException If the symlink cannot be created for any reason.
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        log.debug("Doing reduce ...");
        try {
            Text pageKey = new Text(key.toString().replaceAll("\\.jp2$",""));
            String pid = getDomsPid(pageKey);
            String outputPathString = values.iterator().next().toString();
            String symlinkRoot = context.getConfiguration().get(SYMLINK_ROOTDIR_PATH);
            int symlinkDepth = context.getConfiguration().getInt(SYMLINK_DEPTH, 0);
            String symlinkFilePath;
            if (System.getProperty("java.version").startsWith("1.6.")) {
                symlinkFilePath = LinkUtils.createSymlinkJava6(pid, outputPathString, symlinkRoot, symlinkDepth);
            }  else {
                symlinkFilePath = LinkUtils.createSymlinkJava7(pid, outputPathString, symlinkRoot, symlinkDepth);
            }
            File symlinkFile = new File(symlinkFilePath);
            if (!symlinkFile.exists()) {
                throw new IOException("Failed to create symlink '" + symlinkFilePath + "' to " + key.toString() + ".");
            }
            if (symlinkFile.isDirectory()) {
                throw new IOException("Created a directory where there should have been a symlink: '" + symlinkFilePath + "' for " + key.toString() + ".");
            }
            if (!symlinkFile.getCanonicalPath().equals(new File(outputPathString).getCanonicalPath())) {
                throw new IOException("Symlink does not appear to be a link: " + symlinkFile.getCanonicalPath() + " to " + new File(outputPathString).getCanonicalPath());
            }
            context.write(key, new Text(symlinkFilePath));
        } catch (Exception e) {
            log.error("Error in reduce for " + key.toString() +"", e);
            throw new IOException(e);
        }


    }
}
