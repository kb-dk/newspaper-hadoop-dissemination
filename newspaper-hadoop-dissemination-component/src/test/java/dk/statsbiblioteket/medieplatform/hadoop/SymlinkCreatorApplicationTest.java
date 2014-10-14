package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import dk.statsbiblioteket.medieplatform.autonomous.SymlinkCreatorApplication;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class SymlinkCreatorApplicationTest {

    @Test
    public void testCreateLinks() throws Exception {
        File tempFile = File.createTempFile("foo", null);
        File symlinkRootdir = new File(tempFile.getAbsolutePath() + ".symlink.d");
        symlinkRootdir.mkdirs();
        File batchRootdir = new File(tempFile.getAbsolutePath() + ".batches.d");
        batchRootdir.mkdirs();
        File rt = new File(batchRootdir, "B12345-RT7");
        rt.mkdirs();
        File dir1 = new File(rt, "dir1");
        File dir2 = new File(rt, "dir2");
        File dir3 = new File(dir1, "dir3");
        dir3.mkdirs();
        dir2.mkdirs();
        final File file1 = new File(dir1, "file1.jp2");
        file1.createNewFile();
        final File file2 = new File(dir1, "file2.jp2");
        file2.createNewFile();
        final File filex = new File(dir1, "filex.jp3");
        filex.createNewFile();
        final File file3 = new File(dir2, "file3.jp2");
        file3.createNewFile();
        final File file4 = new File(dir3, "file4.jp2");
        file4.createNewFile();
        EnhancedFedora enhancedFedora = mock(EnhancedFedora.class);
        when(enhancedFedora.findObjectFromDCIdentifier(contains("file1"))).thenReturn(getSingleElemenList("uuid:abcdfoo-bar"));
        when(enhancedFedora.findObjectFromDCIdentifier(contains("file2"))).thenReturn(getSingleElemenList("uuid:1234foo-bar"));
        when(enhancedFedora.findObjectFromDCIdentifier(contains("file3"))).thenReturn(getSingleElemenList("uuid:a2c4foo-bar"));
        when(enhancedFedora.findObjectFromDCIdentifier(contains("file4"))).thenReturn(getSingleElemenList("uuid:1b3dbar-foo"));
        SymlinkCreatorApplication application = new SymlinkCreatorApplication();
        application.createLinks(rt, symlinkRootdir, 4, enhancedFedora, application.getJP2FileFilter());
        File link1 = new File(symlinkRootdir, "a/b/c/d/abcdfoo-bar.jp2");
        assertEquals(link1.getCanonicalPath(), file1.getCanonicalPath());
    }

    private static List<String> getSingleElemenList(String element) {
        List<String> result = new ArrayList<String>();
        result.add(element);
        return result;
    }
}
