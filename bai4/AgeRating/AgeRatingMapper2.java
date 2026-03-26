// Job: forward data

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AgeRatingMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }
}