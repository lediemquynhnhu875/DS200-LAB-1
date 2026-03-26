// Job 2: tính avg theo age group

import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AgeRatingReducer2 extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> movieMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader br = new BufferedReader(
                new FileReader(new File(new Path(cacheFiles[0]).getName())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",\\s*");
                if (f.length >= 2) movieMap.put(f[0].trim(), f[1].trim());
            }
            br.close();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Double> sumMap = new LinkedHashMap<>();
        Map<String, Integer> countMap = new LinkedHashMap<>();

        String[] groups = {"0-18", "18-35", "35-50", "50+"};
        for (String g : groups) {
            sumMap.put(g, 0.0);
            countMap.put(g, 0);
        }

        for (Text val : values) {
            String[] parts = val.toString().split("::");
            if (parts.length < 2) continue;
            String group = parts[0];
            try {
                double rating = Double.parseDouble(parts[1]);
                if (sumMap.containsKey(group)) {
                    sumMap.put(group, sumMap.get(group) + rating);
                    countMap.put(group, countMap.get(group) + 1);
                }
            } catch (NumberFormatException e) {
                // bỏ qua
            }
        }

        String movieTitle = movieMap.getOrDefault(key.toString(), "Unknown(" + key + ")");

        StringBuilder sb = new StringBuilder();
        for (String g : groups) {
            int count = countMap.get(g);
            String avg = count > 0 ? String.format("%.2f", sumMap.get(g) / count) : "N/A";
            if (sb.length() > 0) sb.append(", ");
            sb.append(g).append(": ").append(avg);
        }

        context.write(new Text(movieTitle), new Text("[" + sb.toString() + "]"));
    }
}
