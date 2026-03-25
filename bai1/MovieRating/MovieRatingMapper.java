import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class MovieRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> movieMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Đọc file từ working directory (Hadoop tự symlink file cache về đây)
        File moviesFile = new File("movies.txt");
        if (!moviesFile.exists()) {
            // Thử tìm trong current directory
            throw new IOException("movies.txt not found in working directory: " 
                + new File(".").getAbsolutePath());
        }
        BufferedReader br = new BufferedReader(new FileReader(moviesFile));
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split(",", 3);
            if (parts.length >= 2) {
                movieMap.put(parts[0].trim(), parts[1].trim());
            }
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",");
        if (parts.length < 3) return;

        String movieId = parts[1].trim();
        String ratingStr = parts[2].trim();

        try {
            Float.parseFloat(ratingStr);
            String title = movieMap.getOrDefault(movieId, "Unknown_" + movieId);
            context.write(new Text(title), new Text(ratingStr));
        } catch (NumberFormatException e) {
            // bỏ qua
        }
    }
}