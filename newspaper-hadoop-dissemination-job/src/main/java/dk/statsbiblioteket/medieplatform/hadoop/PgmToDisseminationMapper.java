package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

public class PgmToDisseminationMapper extends ConvertMapper {

    @Override
    protected File getConvertedPath(String dataPath) {
        File file = getDisseminationCopyFile(dataPath);
        file.getParentFile().mkdirs();
        return file;
    }
    
    private File getDisseminationCopyFile(String dataPath) {
        File superPath = new File(getBatchFolder().getParentFile(), new File(dataPath + ".jp2").getName());
        String path = superPath.getAbsolutePath().replaceAll("_", File.separator).replaceAll("\\.jp2.*$", "").concat("-presentation.jp2");
        return new File(path);
    }

    /**
     * Map method, uses the super ConvertMapper to create a dissemination jp2-file. 
     * The temporary pgm-file is removed, and the outputted jp2-file get its size checked to be larger than 0. 
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        removeIntermediateFile(value.toString());
        checkDisseminationCopy(value.toString());
    }
    
    private void removeIntermediateFile(String dataFile) {
        File pgmFile = new File(dataFile);
        pgmFile.delete();
        
    }
    
    private void checkDisseminationCopy(String dataFile) throws IOException {
        File disseminationCopy = getDisseminationCopyFile(dataFile);
        if(!disseminationCopy.exists() || !disseminationCopy.isFile()) {
            String message = "Created dissemination copy does not exist or is no file ('" + disseminationCopy.getAbsolutePath() + "'). " +
                    "Something went wrong here.";
            throw new IOException(message);
        }
        if(!(disseminationCopy.length() > 0)) {
            String message = "Created dissemination copy ('" + disseminationCopy.getAbsolutePath() + "') " +
                    "was of size " + disseminationCopy.length() + ". Something went wrong here.";
            throw new IOException(message);
        } 
    }
}
