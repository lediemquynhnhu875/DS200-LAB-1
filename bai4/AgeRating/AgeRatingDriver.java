import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeRatingDriver {
    public static void main(String[] args) throws Exception {
        // args[0]: ratings dir
        // args[1]: users path
        // args[2]: movies path
        // args[3]: intermediate output
        // args[4]: final output

        Configuration conf = new Configuration();

        // ===== JOB 1: Join ratings + users =====
        Job job1 = Job.getInstance(conf, "Age Rating Join");
        job1.setJarByClass(AgeRatingDriver.class);
        job1.setReducerClass(AgeRatingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]),
            TextInputFormat.class, AgeRatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]),
            TextInputFormat.class, AgeRatingMapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.waitForCompletion(true);

        // ===== JOB 2: Tính avg theo age group =====
        Job job2 = Job.getInstance(conf, "Age Rating Avg");
        job2.setJarByClass(AgeRatingDriver.class);
        job2.setMapperClass(AgeRatingMapper2.class);
        job2.setReducerClass(AgeRatingReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.waitForCompletion(true);
    }
}
