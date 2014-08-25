package dk.statsbiblioteket.medieplatform.hadoop;

import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by csr on 25/08/14.
 */
public class SymlinkCreatorReducerTest {

    public static String TESTROOT;
    public static String ORIGINALS_DIR;
    public static String FINALS_DIR;
    public static String LINKS_DIR;

    public static File testrootDir;
    public static File originalsDir;
    public static File finalsDir;
    public static File linksDir;

    public static Properties properties;
    public static File genericPropertyFile;

    @Mock
    Iterable<Text> mockValues;

    @Mock
    Iterator<Text> mockValuesIterator;

    private static void setFiles() {
        String symlinkRoot = properties.getProperty("symlink.rootdir.path");
        TESTROOT = symlinkRoot + "/testdir";
        ORIGINALS_DIR = symlinkRoot + "/originals";
        FINALS_DIR = symlinkRoot + "/finals";
        LINKS_DIR = symlinkRoot + "/links";
        testrootDir = new File(TESTROOT);
        originalsDir = new File(ORIGINALS_DIR);
        finalsDir = new File(FINALS_DIR);
        linksDir = new File(LINKS_DIR);
    }

    @BeforeMethod
    public void setUp() throws IOException {
        String pathToProperties = System.getProperty("integration.test.newspaper.properties");
        properties = new Properties();
        genericPropertyFile = new File(pathToProperties);
        properties.load(new FileInputStream(genericPropertyFile));
        setFiles();
        tearDown();
        assertTrue(originalsDir.mkdirs(), "Could not create " + originalsDir);
        assertTrue(finalsDir.mkdirs(), "Could not create " + finalsDir);
        assertTrue(linksDir.mkdirs(), "Could not create " + linksDir);
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(testrootDir);
    }

    @Test(groups = "integrationTest")
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

    @Test(groups = "integrationTest")
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
