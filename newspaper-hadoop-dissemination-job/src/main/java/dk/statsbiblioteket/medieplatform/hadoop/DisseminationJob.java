package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * The jpylyzer job. Eats a text file containing paths to jpegs, runs jpylyzer on each and looks up the path in doms to
 * store the result.
 */
public class DisseminationJob implements Tool {


    public static final String JP2K_TO_PGM_COMMAND = "jp2k.to.pgm.command";
    public static final String PGM_TO_JP2K_COMMAND = "pgm.to.jp2k.command";
    public static final String JP2K_TO_PGM_OUTPUT_PATH = "jp2k.to.pgm.output.path";
    public static final String PGM_TO_JP2K_OUTPUT_PATH = "pgm.to.jp2k.output.path";
    private static Logger log = Logger.getLogger(DisseminationJob.class);
    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DisseminationJob(), args);
        System.exit(res);
    }

    /**
     * Run the job with the args
     *
     * @param args first argument is a path to a file listing the jpeg2k files to work on. Second argument is to the
     *             output dir
     *
     * @return return code, 0 is success
     * @throws java.io.IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();
        configuration.setIfUnset(ConfigConstants.DOMS_URL, "http://achernar:7880/fedora");
        configuration.setIfUnset(ConfigConstants.DOMS_USERNAME, "fedoraAdmin");
        configuration.setIfUnset(ConfigConstants.DOMS_PASSWORD, "fedoraAdminPass");


        Job job = Job.getInstance(configuration);
        job.setJobName("Newspaper " + getClass().getSimpleName() + " " + configuration.get(ConfigConstants.BATCH_ID));

        job.setJarByClass(DisseminationJob.class);
        job.setMapperClass(ChainMapper.class);

        ChainMapper.addMapper(
                job,
                WrapperMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                Text.class,
                new Configuration(false));

        /*Original jp2 to temp pgm mapper*/
        Configuration origToPgmMapperConf = new Configuration(false);
        origToPgmMapperConf.set(ConvertMapper.HADOOP_CONVERTER_OUTPUT_PATH, configuration.get(JP2K_TO_PGM_OUTPUT_PATH));
        origToPgmMapperConf.set(ConvertMapper.HADOOP_CONVERTER_PATH, configuration.get(JP2K_TO_PGM_COMMAND));
        origToPgmMapperConf.set(ConvertMapper.HADOOP_CONVERTER_OUTPUT_EXTENSION_PATH, ".pgm");
        ChainMapper.addMapper(
                job, ConvertMapper.class, Text.class, Text.class, Text.class, Text.class, origToPgmMapperConf);


        /* temp pgm to presentation jp2 mapper */
        Configuration pgmToDisseminationMapperConf = new Configuration(false);
        pgmToDisseminationMapperConf.set(ConvertMapper.HADOOP_CONVERTER_OUTPUT_PATH, configuration.get(PGM_TO_JP2K_OUTPUT_PATH));
        pgmToDisseminationMapperConf.set(
                ConvertMapper.HADOOP_CONVERTER_PATH, configuration.get(PGM_TO_JP2K_COMMAND));
        pgmToDisseminationMapperConf.set(ConvertMapper.HADOOP_CONVERTER_OUTPUT_EXTENSION_PATH, ".jp2");
        ChainMapper.addMapper(
                job, PgmToDisseminationMapper.class, Text.class, Text.class, Text.class, Text.class, pgmToDisseminationMapperConf);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        int filesPerMapTask = configuration.getInt(ConfigConstants.FILES_PER_MAP_TASK, 1);
        NLineInputFormat.setNumLinesPerSplit(job, filesPerMapTask);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        log.info(job);
        return result ? 0 : 1;

    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }
}
