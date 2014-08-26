package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class LinkUtilsTest {

    /**
     * Test that the utility for creating a symlink does indeed create a symlink, with the expected depth, name
     * and suffix, and that it is indeed a link to the original file.
     * @throws Exception
     */
    @Test
    public void testCreateSymlinkJava6() throws Exception {
        File tempFile = File.createTempFile("foo", "bar");
        File tempDir = new File(tempFile.getAbsolutePath() + ".d");
        tempDir.mkdirs();
        File dataFile = new File(tempDir, "helloworld.blip");
        dataFile.createNewFile();
        File symlinkRoot = new File(tempDir, "symlinks");
        String pid = "uuid:xyzaestfu";
        String link = LinkUtils.createSymlinkJava6(pid, dataFile.getAbsolutePath(), symlinkRoot.getAbsolutePath(), 4);
        assertEquals(new File(link).getCanonicalPath(), dataFile.getCanonicalPath());
        assertEquals(FilenameUtils.getExtension(link), FilenameUtils.getExtension(dataFile.getName()));
        assertTrue(link.endsWith("symlinks/x/y/z/a/xyzaestfu.blip"));
    }

    /**
     * Test that the utility for creating a symlink does indeed create a symlink, with the expected depth, name
     * and suffix, and that it is indeed a link to the original file.
     * @throws Exception
     */
    @Test
    public void testCreateSymlinkJava7() throws Exception {
        File tempFile = File.createTempFile("foo", "bar");
        File tempDir = new File(tempFile.getAbsolutePath() + ".d");
        tempDir.mkdirs();
        File dataFile = new File(tempDir, "helloworld.blip");
        dataFile.createNewFile();
        File symlinkRoot = new File(tempDir, "symlinks");
        String pid = "uuid:xyzaestfu";
        String link = LinkUtils.createSymlinkJava7(pid, dataFile.getAbsolutePath(), symlinkRoot.getAbsolutePath(), 4);
        assertEquals(new File(link).getCanonicalPath(), dataFile.getCanonicalPath());
        assertEquals(FilenameUtils.getExtension(link), FilenameUtils.getExtension(dataFile.getName()));
        assertTrue(link.endsWith("symlinks/x/y/z/a/xyzaestfu.blip"));
    }
}
