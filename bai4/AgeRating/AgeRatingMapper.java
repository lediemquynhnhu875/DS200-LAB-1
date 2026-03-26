import java.io.*;
import java.lang.reflect.Method;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AgeRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

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

    private String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",\\s*");

        if (fileType.equals("users")) {
            // UserID, Gender, Age, Occupation, Zip
            if (fields.length >= 3) {
                String userID = fields[0].trim();
                try {
                    int age = Integer.parseInt(fields[2].trim());
                    String ageGroup = getAgeGroup(age);
                    context.write(new Text(userID), new Text("U::" + ageGroup));
                } catch (NumberFormatException e) {
                    // bỏ qua dòng lỗi
                }
            }
        } else if (fileType.equals("ratings")) {
            // UserID, MovieID, Rating, Timestamp
            if (fields.length >= 3) {
                String userID = fields[0].trim();
                String movieID = fields[1].trim();
                String rating = fields[2].trim();
                context.write(new Text(userID), new Text("R::" + movieID + "::" + rating));
            }
        }
    }
}
