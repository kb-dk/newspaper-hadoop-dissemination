package dk.statsbiblioteket.medieplatform.autonomous;

import dk.statsbiblioteket.medieplatform.hadoop.DisseminationJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertTrue;

public class DisseminationRunnableComponentTest {


    @Test(groups = "integrationTest", enabled = true)
    public void testDoWorkOnBatch() throws Exception {
        String pathToProperties = System.getProperty("integration.test.newspaper.properties");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToProperties));

        Batch batch = new Batch("400022028241");

        properties.setProperty(
                ConfigConstants.JOB_FOLDER, "inputFiles-dissemination");
        properties.setProperty(
                ConfigConstants.PREFIX,
                "/net/zone1.isilon.sblokalnet/ifs/archive/bitmag-devel01-data/cache/avisbits/perm/avis/");
        properties.setProperty(ConfigConstants.HADOOP_USER, "newspapr");
        properties.setProperty(ConfigConstants.FILES_PER_MAP_TASK, "5");


        properties.setProperty(DisseminationJob.JP2K_TO_PGM_COMMAND, "kdu_expand -num_threads 1 -fprec 8M");
        properties.setProperty(DisseminationJob.JP2K_TO_PGM_OUTPUT_PATH, "/tmp/");
        properties.setProperty(
                DisseminationJob.PGM_TO_JP2K_COMMAND,
                "kdu_compress -rate 0.7,0.5,0.35,0.25,0.18,0.125,0.088,0.0625,0.04419,0.03125,0.0221,0.015625 Cmodes=BYPASS Cuse_sop=yes Cuse_eph=yes Clevels=6 Cprecincts={256,256},{256,256},{128,128} Corder=RPCL ORGtparts=R Cblk={64,64} ORGgen_plt=yes Stiles={1024,1024}");
        properties.setProperty(DisseminationJob.PGM_TO_JP2K_OUTPUT_PATH, "/avis-show/");


        clean(properties.getProperty(ConfigConstants.JOB_FOLDER));

        DisseminationRunnableComponent component = new DisseminationRunnableComponent(properties);
        ResultCollector resultCollector = new ResultCollector("tool", "version");

        component.doWorkOnBatch(batch, resultCollector);
        assertTrue(resultCollector.isSuccess(), resultCollector.toReport());
    }

    private void clean(String jobFolder) throws IOException, InterruptedException {
        Configuration conf = new Configuration(true);
        String user = conf.get(ConfigConstants.HADOOP_USER, "newspapr");
        FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, user);
        fs.delete(new Path(jobFolder), true);
    }

}
