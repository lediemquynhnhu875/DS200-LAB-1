// Job: Chỉ forward data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;

public class GenderRatingMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Input: "MovieID\tGender::Rating"
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }
}