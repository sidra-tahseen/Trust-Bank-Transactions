package trustbank.problem4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver for Problem 4: Weekend vs Weekday Transaction Volume by Branch.
 * Configures and executes the MapReduce job on Hadoop YARN.
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class WeekendWeekdayDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WeekendWeekdayDriver <input_path> <output_path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TrustBank - Weekend vs Weekday Transaction Volume (Problem 4)");

        job.setJarByClass(WeekendWeekdayDriver.class);

        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(WeekendWeekdayMapper.class);
        job.setCombinerClass(WeekendWeekdayCombiner.class);
        job.setReducerClass(WeekendWeekdayReducer.class);

        // Set Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VolumeWritable.class);

        // Set Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Single reducer ensures unified global ranking and analytics in cleanup()
        job.setNumReduceTasks(1);

        // Set Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Clean up previous output directory if it exists
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.out.println("Cleaning up existing output directory: " + outputPath);
            fs.delete(outputPath, true);
        }

        System.out.println("=================================================================");
        System.out.println("TrustBank Analytics - Submitting Problem 4 MapReduce Job to YARN");
        System.out.println("Input Path:  " + inputPath);
        System.out.println("Output Path: " + outputPath);
        System.out.println("=================================================================");

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("\nJob completed successfully!");
            System.out.println("Total Records:     " + job.getCounters().findCounter(WeekendWeekdayMapper.TransactionCounters.TOTAL_RECORDS).getValue());
            System.out.println("Valid Records:     " + job.getCounters().findCounter(WeekendWeekdayMapper.TransactionCounters.VALID_RECORDS).getValue());
            System.out.println("Weekday Records:   " + job.getCounters().findCounter(WeekendWeekdayMapper.TransactionCounters.WEEKDAY_COUNT).getValue());
            System.out.println("Weekend Records:   " + job.getCounters().findCounter(WeekendWeekdayMapper.TransactionCounters.WEEKEND_COUNT).getValue());
            System.out.println("Malformed Records: " + job.getCounters().findCounter(WeekendWeekdayMapper.TransactionCounters.MALFORMED_RECORDS).getValue());
            return 0;
        } else {
            System.err.println("\nJob execution failed!");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WeekendWeekdayDriver(), args);
        System.exit(exitCode);
    }
}
