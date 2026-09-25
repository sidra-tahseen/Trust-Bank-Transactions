package trustbank.problem5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HighValueTrendDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println(
                "Usage: HighValueTrendDriver <input_path> <output_path>"
            );
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(
            conf,
            "TrustBank - Day-wise High Value Transaction Trend (Problem 5)"
        );

        job.setJarByClass(HighValueTrendDriver.class);

        // Set Mapper and Reducer
        job.setMapperClass(HighValueTrendMapper.class);
        job.setReducerClass(HighValueTrendReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
            System.out.println(
                "Cleaning up existing output directory: " + outputPath
            );
            fs.delete(outputPath, true);
        }

        System.out.println("=================================================================");
        System.out.println(
            "TrustBank Analytics - Submitting Problem 5 MapReduce Job to YARN"
        );
        System.out.println("Input Path:  " + inputPath);
        System.out.println("Output Path: " + outputPath);
        System.out.println("=================================================================");

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("\nJob completed successfully!");
            return 0;
        } else {
            System.err.println("\nJob execution failed!");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
            new Configuration(),
            new HighValueTrendDriver(),
            args
        );

        System.exit(exitCode);
    }
}
