// Job 1: Join UserID → lấy (MovieID, Gender, Rating)

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;

public class GenderRatingReducer extends Reducer<Text, Text, Text, Text> {

    // movieTitle lookup — load từ DistributedCache
    private Map<String, String> movieMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Đọc movies.txt từ local cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            readMovieFile(new File(new Path(cacheFiles[0]).getName()), movieMap);
        }
    }

    private void readMovieFile(File file, Map<String, String> map) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split("::");
            if (fields.length >= 2) {
                map.put(fields[0], fields[1]); // MovieID -> Title
            }
        }
        br.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String gender = null;
        List<String> ratings = new ArrayList<>();

        // Phân loại records
        for (Text val : values) {
            String v = val.toString();
            if (v.startsWith("U::")) {
                gender = v.substring(3); // "M" hoặc "F"
            } else if (v.startsWith("R::")) {
                ratings.add(v.substring(3)); // "MovieID::Rating"
            }
        }

        // Nếu không có thông tin giới tính thì bỏ qua
        if (gender == null) return;

        // Ghi ra từng cặp (MovieID, Gender::Rating)
        for (String r : ratings) {
            String[] parts = r.split("::");
            if (parts.length >= 2) {
                String movieID = parts[0];
                String rating = parts[1];
                // Output key: MovieID, value: "Gender::Rating"
                context.write(new Text(movieID), new Text(gender + "::" + rating));
            }
        }
    }
}