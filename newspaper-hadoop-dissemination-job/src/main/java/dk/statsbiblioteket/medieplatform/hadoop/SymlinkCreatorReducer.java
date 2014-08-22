package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger log = LoggerFactory.getLogger(SymlinkCreatorReducer.class);

    /**
     * Creates a symlink from the doms-pid to the dissemination file.
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
            log.debug("Creating a symlink for {} to {}.", pid, outputPathString);
            String filename = pid.replace("uuid:","") + ".jp2";
            String symlinkRoot = context.getConfiguration().get("symlink.rootdir.path");
            List<String> symlinkDirPathElements = new ArrayList<String>();
            int symlinkDepth = context.getConfiguration().getInt("symlink.depth", 0);
            for (int i = 0; i < symlinkDepth; i++) {
                symlinkDirPathElements.add(filename.charAt(i) + "");
            }
            Path symlinkDirPath = Paths.get(symlinkRoot, (String[]) symlinkDirPathElements.toArray());
            log.debug("Creating directory {} (if necessary).", symlinkDirPath.toString());
            Files.createDirectories(symlinkDirPath);
            List<String> symlinkFilePathElements = symlinkDirPathElements;
            symlinkFilePathElements.add(filename);
            Path symlinkFilePath = Paths.get(symlinkRoot, (String[]) symlinkFilePathElements.toArray());
            log.debug("Creating symlink {} for {}.", symlinkFilePath.toString(), outputPathString);
            Files.createSymbolicLink(symlinkFilePath, Paths.get(outputPathString));
            context.write(key, new Text(symlinkFilePath.toFile().getAbsolutePath()));
        } catch (Exception e) {
            log.error("Error in reduce for " + key.toString() +"", e);
            throw new IOException(e);
        }
    }
}
