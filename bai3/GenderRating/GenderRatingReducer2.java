// Job 2: Group theo MovieID → tính avg theo Male/Female

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;

public class GenderRatingReducer2 extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> movieMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader br = new BufferedReader(new FileReader(
                new File(new Path(cacheFiles[0]).getName())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",\\s*");
                if (f.length >= 2) movieMap.put(f[0], f[1]);
            }
            br.close();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double maleSum = 0, femaleSum = 0;
        int maleCount = 0, femaleCount = 0;

        for (Text val : values) {
            String[] parts = val.toString().split("::");
            if (parts.length < 2) continue;
            String gender = parts[0];
            double rating = Double.parseDouble(parts[1]);

            if (gender.equals("M")) {
                maleSum += rating;
                maleCount++;
            } else if (gender.equals("F")) {
                femaleSum += rating;
                femaleCount++;
            }
        }

        String movieTitle = movieMap.getOrDefault(key.toString(), "Unknown");

        String maleAvg = maleCount > 0
            ? String.format("%.2f", maleSum / maleCount) : "N/A";
        String femaleAvg = femaleCount > 0
            ? String.format("%.2f", femaleSum / femaleCount) : "N/A";

        context.write(new Text(movieTitle),
            new Text("Male: " + maleAvg + ", Female: " + femaleAvg));
    }
}