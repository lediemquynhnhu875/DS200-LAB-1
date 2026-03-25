import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class GenreRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Map: MovieID -> Genres (ví dụ: "Action|Comedy|Drama")
    private HashMap<String, String> movieGenreMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File moviesFile = new File("movies.txt");
        if (!moviesFile.exists()) {
            throw new IOException("movies.txt not found in working directory: "
                + new File(".").getAbsolutePath());
        }
        BufferedReader br = new BufferedReader(new FileReader(moviesFile));
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            // Schema: MovieID, Title, Genres
            String[] parts = line.split(",", 3);
            if (parts.length >= 3) {
                String movieId = parts[0].trim();
                String genres  = parts[2].trim(); // "Action|Comedy|Drama"
                movieGenreMap.put(movieId, genres);
            }
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Schema: UserID, MovieID, Rating, Timestamp
        String[] parts = line.split(",");
        if (parts.length < 3) return;

        String movieId  = parts[1].trim();
        String ratingStr = parts[2].trim();

        try {
            Float.parseFloat(ratingStr); // kiểm tra hợp lệ
            String genres = movieGenreMap.get(movieId);
            if (genres == null) return;

            // Tách từng thể loại và emit riêng
            for (String genre : genres.split("\\|")) {
                genre = genre.trim();
                if (!genre.isEmpty()) {
                    context.write(new Text(genre), new Text(ratingStr));
                }
            }
        } catch (NumberFormatException e) {
            // bỏ qua dòng lỗi
        }
    }
}