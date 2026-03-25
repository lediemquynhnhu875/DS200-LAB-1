import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class GenderRatingDriver {
    public static void main(String[] args) throws Exception {
        // args[0]: ratings input dir
        // args[1]: users input path
        // args[2]: movies.txt path
        // args[3]: intermediate output
        // args[4]: final output

        Configuration conf = new Configuration();

        // ============ JOB 1: Join ratings + users ============
        Job job1 = Job.getInstance(conf, "Gender Rating Join");
        job1.setJarByClass(GenderRatingDriver.class);
        job1.setMapperClass(GenderRatingMapper.class);
        job1.setReducerClass(GenderRatingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Thêm movies.txt vào DistributedCache
        job1.addCacheFile(new URI(args[2]));

        // Input: ratings + users
        MultipleInputs.addInputPath(job1, new Path(args[0]),
            TextInputFormat.class, GenderRatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]),
            TextInputFormat.class, GenderRatingMapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.waitForCompletion(true);

        // ============ JOB 2: Tính avg theo giới tính ============
        Job job2 = Job.getInstance(conf, "Gender Rating Avg");
        job2.setJarByClass(GenderRatingDriver.class);
        job2.setMapperClass(GenderRatingMapper2.class);
        job2.setReducerClass(GenderRatingReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.waitForCompletion(true);
    }
}