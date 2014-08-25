package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by csr on 25/08/14.
 */
public class SymlinkCreatorReducerTest {

    public static final String TESTROOT = "testdir";
    public static final String ORIGINALS_DIR = "testdir/originals";
    public static final String FINALS_DIR = "testdir/finals";
    public static final String LINKS_DIR = "testdir/links";

    public static File testrootDir = new File(TESTROOT);
    public static File originalsDir = new File(ORIGINALS_DIR);
    public static File finalsDir = new File(FINALS_DIR);
    public static File linksDir = new File(LINKS_DIR);

    @Mock
    Iterable<Text> mockValues;

    @Mock
    Iterator<Text> mockValuesIterator;

    @BeforeMethod
    public void setUp() throws IOException {
        tearDown();
        originalsDir.mkdirs();
        finalsDir.mkdirs();
        linksDir.mkdirs();
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(testrootDir);
    }

    @Test
    public void testReduceJava7() throws Exception {
        File originalFile = new File(originalsDir, "foobar.jp2");
        originalFile.createNewFile();
        File finalFile = new File(finalsDir, "foobar_final.jp2");
        finalFile.createNewFile();
        final String domsPid = "uuid:aefg0103-2828282-919191";
        SymlinkCreatorReducer reducer = new SymlinkCreatorReducer() {
            @Override
            protected String getDomsPid(Text key) throws BackendInvalidCredsException, BackendMethodFailedException {
                return domsPid;
            }
        };
        Text key = new Text(originalFile.getAbsolutePath());
        Reducer.Context mockContext = mock(Reducer.Context.class);
        Configuration mockConfiguration = mock(Configuration.class);
        when(mockConfiguration.getInt(SymlinkCreatorReducer.SYMLINK_DEPTH, 0)).thenReturn(4);
        when(mockConfiguration.get(SymlinkCreatorReducer.SYMLINK_ROOTDIR_PATH)).thenReturn(linksDir.getAbsolutePath());
        when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
        when(mockValuesIterator.next()).thenReturn(new Text(finalFile.getAbsolutePath()));
        when(mockValues.iterator()).thenReturn(mockValuesIterator);
        reducer.reduceJava7(key, mockValues, mockContext);
        File linkFile = new File(linksDir, "a/e/f/g/aefg0103-2828282-919191.jp2");
        assertTrue(linkFile.exists());
        assertEquals(linkFile.getCanonicalPath(), finalFile.getCanonicalPath());
    }

    @Test
    public void testReduceJava6() throws Exception {
        File originalFile = new File(originalsDir, "foobar.jp2");
        originalFile.createNewFile();
        File finalFile = new File(finalsDir, "foobar_final.jp2");
        finalFile.createNewFile();
        final String domsPid = "uuid:aefg0103-2828282-919191";
        SymlinkCreatorReducer reducer = new SymlinkCreatorReducer() {
            @Override
            protected String getDomsPid(Text key) throws BackendInvalidCredsException, BackendMethodFailedException {
                return domsPid;
            }
        };
        Text key = new Text(originalFile.getAbsolutePath());
        Reducer.Context mockContext = mock(Reducer.Context.class);
        Configuration mockConfiguration = mock(Configuration.class);
        when(mockConfiguration.getInt(SymlinkCreatorReducer.SYMLINK_DEPTH, 0)).thenReturn(4);
        when(mockConfiguration.get(SymlinkCreatorReducer.SYMLINK_ROOTDIR_PATH)).thenReturn(linksDir.getAbsolutePath());
        when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
        when(mockValuesIterator.next()).thenReturn(new Text(finalFile.getAbsolutePath()));
        when(mockValues.iterator()).thenReturn(mockValuesIterator);
        reducer.reduceJava6(key, mockValues, mockContext);
        File linkFile = new File(linksDir, "a/e/f/g/aefg0103-2828282-919191.jp2");
        assertTrue(linkFile.exists());
        assertEquals(linkFile.getCanonicalPath(), finalFile.getCanonicalPath());
    }


}
