package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.util.console.ProcessRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Input is line-number, line text. The text is the path to a file to run jpylyzer on
 * Output is line text, jpylyzer output xml
 */
public class DisseminationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(DisseminationMapper.class);

    /**
     * run command on the given file
     * @param dataPath     the path to the jp2 file
     * @param commandPath the path to the executable
     *
     * @return the path to the converted file
     * @throws java.io.IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    protected static String convert(String dataPath, String commandPath) throws IOException {
        ProcessRunner runner = new ProcessRunner(commandPath, dataPath);
        log.info("Running command '" + commandPath + " " + dataPath + "'");
        Map<String, String> myEnv = new HashMap<String, String>(System.getenv());
        runner.setEnviroment(myEnv);
        runner.setOutputCollectionByteSize(Integer.MAX_VALUE);

        String resultPath = getConvertedPath(dataPath);

        //this call is blocking
        runner.run();

        //we could probably do something more clever with returning the output while the command is still running.
        if (runner.getReturnCode() == 0) {
            return resultPath;
        } else {
            String message
                    = "failed to run jpylyzer, returncode:" + runner.getReturnCode() + ", stdOut:" + runner.getProcessOutputAsString() + " stdErr:" + runner.getProcessErrorAsString();
            log.error(message);
            throw new IOException(message);
        }
    }

    private static String getConvertedPath(String dataPath) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String commandPath = context.getConfiguration()
                                         .get(ConfigConstants.JPYLYZER_PATH);


            String disseminationCopyPath = convert(value.toString(), commandPath);

            context.write(value, new Text(disseminationCopyPath));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }



}
