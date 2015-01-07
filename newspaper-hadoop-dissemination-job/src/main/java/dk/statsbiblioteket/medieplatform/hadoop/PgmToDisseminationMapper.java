package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

public class PgmToDisseminationMapper extends ConvertMapper {

    @Override
    protected File getConvertedPath(String dataPath) {
        File superPath = new File(getBatchFolder().getParentFile(), new File(dataPath + ".jp2").getName());
        String path = superPath.getAbsolutePath().replaceAll("_", File.separator).replaceAll("\\.jp2.*$", "").concat("-presentation.jp2");
        File file = new File(path);
        file.getParentFile().mkdirs();



    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        File pgmFile = new File(value.toString());
        pgmFile.delete();
    }
}
