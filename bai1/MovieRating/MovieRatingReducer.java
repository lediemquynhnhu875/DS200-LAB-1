import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.io.IOException;

public class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {

    // Biến lớp để tìm phim điểm cao nhất
    private static String maxMovie = "";
    private static float maxRating = -1f;
    private static int maxCount = 0;

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

        // Cập nhật biến lớp (chỉ xét phim >= 5 lượt)
        if (count >= 5 && avg > maxRating) {
            maxRating = avg;
            maxMovie = key.toString();
            maxCount = count;
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        if (!maxMovie.isEmpty()) {
            String result = String.format(
                "%s is the highest rated movie with an average rating of %.2f" +
                " among movies with at least 5 ratings.",
                maxMovie, maxRating
            );
            context.write(new Text("---"), new Text(result));
        }
    }
}