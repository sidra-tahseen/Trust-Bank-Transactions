package trustbank.p3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * TrustBank Analytics - Problem 3
 * Team Member: Boya Abhinay (Member 3) | HT No: 160124733315
 *
 * Driver:
 * Sets up and configures the MapReduce job to calculate average transaction amount by type.
 * Usage:
 *   hadoop jar trustbank-p3.jar trustbank.p3.AvgTransactionDriver <input_path> <output_path>
 */
public class AvgTransactionDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("==================================================================");
            System.err.println("Error: Insufficient arguments!");
            System.err.println("Usage: hadoop jar trustbank-p3.jar trustbank.p3.AvgTransactionDriver <input_path> <output_path>");
            System.err.println("Example: hadoop jar trustbank-p3.jar trustbank.p3.AvgTransactionDriver /dataset/transactions.csv /output/p3");
            System.err.println("==================================================================");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TrustBank - P3: Average Transaction Amount by Type");

        job.setJarByClass(AvgTransactionDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AvgTransactionMapper.class);
        job.setReducerClass(AvgTransactionReducer.class);

        // Set Mapper output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set Job output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Delete output directory if it exists to allow re-runs
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.out.println("Notice: Output directory " + outputPath + " already exists. Deleting it for fresh execution...");
            fs.delete(outputPath, true);
        }

        System.out.println("==================================================================");
        System.out.println("TrustBank Analytics - Team 4");
        System.out.println("Member 3: Boya Abhinay (HT No: 160124733315)");
        System.out.println("Problem 3: Average Transaction Amount by Transaction Type");
        System.out.println("Input Path:  " + inputPath);
        System.out.println("Output Path: " + outputPath);
        System.out.println("==================================================================");

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("\n>>> MapReduce Job Completed Successfully! <<<");
            return 0;
        } else {
            System.err.println("\n>>> MapReduce Job Failed! <<<");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AvgTransactionDriver(), args);
        System.exit(exitCode);
    }
}
