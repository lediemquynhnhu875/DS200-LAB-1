import java.io.*;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GenderRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String fileType = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            InputSplit split = context.getInputSplit();
            String splitStr = split.toString();

            if (splitStr.contains("users")) {
                fileType = "users";
            } else if (splitStr.contains("ratings")) {
                fileType = "ratings";
            }

            if (fileType.isEmpty()) {
                while (!(split instanceof FileSplit)) {
                    try {
                        Method m = split.getClass().getDeclaredMethod("getInputSplit");
                        m.setAccessible(true);
                        split = (InputSplit) m.invoke(split);
                    } catch (Exception e) {
                        break;
                    }
                }
                if (split instanceof FileSplit) {
                    String path = ((FileSplit) split).getPath().toString();
                    if (path.contains("users")) fileType = "users";
                    else if (path.contains("ratings")) fileType = "ratings";
                }
            }

        } catch (Exception e) {
            throw new IOException("Setup error", e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",\\s*");

        if (fileType.equals("users")) {
            if (fields.length >= 2) {
                context.write(new Text(fields[0].trim()), new Text("U::" + fields[1].trim()));
            }
        } else if (fileType.equals("ratings")) {
            if (fields.length >= 3) {
                context.write(new Text(fields[0].trim()),
                    new Text("R::" + fields[1].trim() + "::" + fields[2].trim()));
            }
        }
    }
}
