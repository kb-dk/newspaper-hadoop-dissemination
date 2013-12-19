package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

public class PgmToDisseminationMapper extends ConvertMapper {

    @Override
    protected File getConvertedPath(String dataPath) {
        //TODO
        return super.getConvertedPath(dataPath);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        File pgmFile = new File(value.toString());
        pgmFile.delete();
    }
}
