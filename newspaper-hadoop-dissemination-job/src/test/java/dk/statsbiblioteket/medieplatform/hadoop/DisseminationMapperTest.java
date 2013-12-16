package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class DisseminationMapperTest {


    @BeforeClass
    public void setUp() {
        //JpylyzerMapper mapper = new JpylyzerMapper("src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
    }

    @Test
    public void testSimplest() throws IOException {
        MapDriver<LongWritable, Text, Text, Text> mapDriver;
        DisseminationMapper mapper = new DisseminationMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getConfiguration().set(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.JPYLYZER_PATH, "echo");

        mapDriver.withInput(new LongWritable(1), new Text("ein"));
        mapDriver.withOutput(new Text("ein"), new Text("ein"));
        mapDriver.runTest();
    }


    private String getAbsolutePath(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getAbsolutePath();
    }

}
