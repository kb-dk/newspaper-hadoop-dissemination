package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class SymlinkCreatorReducer extends AbstractDomsReducer {

    public static final String SYMLINK_ROOTDIR_PATH = "symlink.rootdir.path";
    public static final String SYMLINK_DEPTH = "symlink.depth";
    private static Logger log = LoggerFactory.getLogger(SymlinkCreatorReducer.class);


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
        log.debug("Doing reduce ...");
        try {
            String pid = getDomsPid(key);
            String outputPathString = values.iterator().next().toString();
            String symlinkRoot = context.getConfiguration().get(SYMLINK_ROOTDIR_PATH);
            int symlinkDepth = context.getConfiguration().getInt(SYMLINK_DEPTH, 0);
            String symlinkFilePath =null;
            if (System.getProperty("java.version").startsWith("1.6.")) {
                symlinkFilePath = LinkUtils.createSymlinkJava6(pid, outputPathString, symlinkRoot, symlinkDepth);
            }  else {
                symlinkFilePath = LinkUtils.createSymlinkJava7(pid, outputPathString, symlinkRoot, symlinkDepth);
            }
            context.write(key, new Text(symlinkFilePath));
        } catch (Exception e) {
            log.error("Error in reduce for " + key.toString() +"", e);
            throw new IOException(e);
        }


    }
}
