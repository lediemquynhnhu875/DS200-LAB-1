import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.io.IOException;

public class GenreRatingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0f;
        int count = 0;

        for (Text val : values) {
            try {
                sum += Float.parseFloat(val.toString().trim());
                count++;
            } catch (NumberFormatException e) {
                // bỏ qua
            }
        }

        if (count == 0) return;

        float avg = sum / count;
        String output = String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count);
        context.write(key, new Text(output));
    }
}