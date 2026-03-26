// Job 1: join ra MovieID, AgeGroup, Rating

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AgeRatingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String ageGroup = null;
        List<String> ratings = new ArrayList<>();

        for (Text val : values) {
            String v = val.toString();
            if (v.startsWith("U::")) {
                ageGroup = v.substring(3);
            } else if (v.startsWith("R::")) {
                ratings.add(v.substring(3));
            }
        }

        if (ageGroup == null) return;

        for (String r : ratings) {
            String[] parts = r.split("::");
            if (parts.length >= 2) {
                String movieID = parts[0];
                String rating = parts[1];
                context.write(new Text(movieID), new Text(ageGroup + "::" + rating));
            }
        }
    }
}
