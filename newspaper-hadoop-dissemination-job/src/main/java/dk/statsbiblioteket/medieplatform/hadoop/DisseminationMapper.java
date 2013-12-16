package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.util.console.ProcessRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
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
     *
     * @param dataPath     the path to the jp2 file
     * @param commandPath the path to the executable
     *
     * @param outputFolder
     * @return the path to the converted file
     * @throws java.io.IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    protected static String convert(String dataPath, String commandPath, String outputFolder) throws IOException {

        String resultPath = getConvertedPath(dataPath,outputFolder);
        String[] commandLine = makeCommandLine(dataPath,commandPath,resultPath);
        ProcessRunner runner = new ProcessRunner(commandLine);

        Map<String, String> myEnv = new HashMap<String, String>(System.getenv());
        runner.setEnviroment(myEnv);
        runner.setOutputCollectionByteSize(Integer.MAX_VALUE);

        //this call is blocking
        runner.run();

        //we could probably do something more clever with returning the output while the command is still running.
        if (runner.getReturnCode() == 0) {
            return resultPath;
        } else {
            String message
                    = "failed to run  returncode:" + runner.getReturnCode() + ", stdOut:" + runner.getProcessOutputAsString() + " stdErr:" + runner.getProcessErrorAsString();
            log.error(message);
            throw new IOException(message);
        }
    }

    private static String[] makeCommandLine(String dataPath, String commandPath, String resultFile) {
        //TODO IMPLEMENT THIS
        return new String[]{
                commandPath,
                "-i",
                dataPath,
                "-o",
                resultFile
        };
    }

    private static String getConvertedPath(String dataPath, String outputFolder) {
        //TODO IMPLEMENT THIS
        return new File(outputFolder,new File(dataPath).getName()).getPath(); //Absolute path?
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {

            String outputFolder = context.getConfiguration()
                                                     .get(ConfigConstants.DISSEMINATION_FOLDER);
            String commandPath = context.getConfiguration()
                                         .get(ConfigConstants.KAKADU_PATH);

            String disseminationCopyPath = convert(value.toString(), commandPath, outputFolder);

            context.write(value, new Text(disseminationCopyPath));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }



}
