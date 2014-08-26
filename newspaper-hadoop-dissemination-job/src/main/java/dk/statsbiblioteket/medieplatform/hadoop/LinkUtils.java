package dk.statsbiblioteket.medieplatform.hadoop;
import org.apache.commons.io.FilenameUtils;
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
 * Utility class for reusable code for creating symlinks.
 */
public class LinkUtils {

    private static Logger log = LoggerFactory.getLogger(LinkUtils.class);

    /**
     * Creates a symlink for a given file.
     * If we have a doms pid: uuid:aefg998282 ...
     * corresponding to a file: foobar.jp2
     * then calling this method with these as the first two parameters will create a symlink of the form
     * {symlinkRoot}/a/e/f/g/aefg998282....jp2 -> foobar.jp2
     * the final two parameters determine the rootdir for the symlinks and the depth
     * of the directory nesting. The file extension is copied to the link.
     *
     * @param pid
     * @param outputPathString
     * @param symlinkRoot
     * @param symlinkDepth
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static String createSymlinkJava6(String pid, String outputPathString, String symlinkRoot, int symlinkDepth) throws IOException, InterruptedException {
        File symlinkRootDir = new File(symlinkRoot);
        log.debug("Creating a symlink for {} to {}.", pid, outputPathString);
        String extension = FilenameUtils.getExtension(outputPathString);
        String filename = pid.replace("uuid:","") + "." + extension;
        String symlinkPath = "";
        for (int i = 0; i < symlinkDepth; i++) {
            symlinkPath += filename.charAt(i) + "/";
        }
        File symlinkDir;
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
        return symlinkFileAbsolutePath;
    }

    /**
        * Creates a symlink for a given file.
        * If we have a doms pid: uuid:aefg998282 ...
        * corresponding to a file: foobar.jp2
        * then calling this method with these as the first two parameters will create a symlink of the form
        * {symlinkRoot}/a/e/f/g/aefg998282....jp2 -> foobar.jp2
        * the final two parameters determine the rootdir for the symlinks and the depth
        * of the directory nesting.
        *
        * @param pid
        * @param outputPathString
        * @param symlinkRoot
        * @param symlinkDepth
        * @return
        * @throws IOException
        * @throws InterruptedException
        */
    public static String createSymlinkJava7(String pid, String outputPathString, String symlinkRoot, int symlinkDepth) throws IOException {
        log.debug("Creating a symlink for {} to {}.", pid, outputPathString);
        String extension = FilenameUtils.getExtension(outputPathString);
        String filename = pid.replace("uuid:","") + "." + extension;
        List<String> symlinkDirPathElements = new ArrayList<String>();
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
        return symlinkFilePath.toString();
    }


}
