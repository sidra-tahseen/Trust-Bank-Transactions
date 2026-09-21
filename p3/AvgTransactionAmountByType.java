import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ============================================================================
 * TrustBank Big Data Analytics - Case Study
 * Team: Team 4
 * Member: Boya Abhinay (Member 3)
 * Hall Ticket No: 160124733315
 *
 * Problem 3: What is the average transaction amount by transaction type
 *            (deposit / withdrawal / transfer)?
 * ============================================================================
 *
 * Standalone All-In-One MapReduce Implementation
 * Contains:
 *   1. AvgMapper (Static Inner Class)
 *   2. AvgReducer (Static Inner Class)
 *   3. Main Driver Method
 *
 * Compilation:
 *   javac -classpath $(hadoop classpath) -d . AvgTransactionAmountByType.java
 *   jar -cvf trustbank-p3.jar *.class
 *
 * Execution:
 *   hadoop jar trustbank-p3.jar AvgTransactionAmountByType /path/to/transactions.csv /path/to/output
 * ============================================================================
 */
public class AvgTransactionAmountByType {

    /**
     * Mapper Phase:
     * Reads CSV rows, skips header, extracts transaction type and amount.
     */
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text transactionType = new Text();
        private final DoubleWritable transactionAmount = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty() || line.startsWith("txnId") || line.toLowerCase().contains("accountid")) {
                return;
            }

            String[] tokens = line.split(",");
            if (tokens.length >= 5) {
                String amountStr = tokens[3].trim();
                String typeStr = tokens[4].trim().toLowerCase();

                try {
                    double amount = Double.parseDouble(amountStr);
                    transactionType.set(typeStr);
                    transactionAmount.set(amount);
                    context.write(transactionType, transactionAmount);
                } catch (NumberFormatException e) {
                    context.getCounter("TrustBank_P3", "MALFORMED_AMOUNT").increment(1);
                }
            }
        }
    }

    /**
     * Reducer Phase:
     * Groups by transaction type, computes sum of amounts and total count,
     * calculates average = sum / count, and emits (type, average).
     */
    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                result.set(String.format(Locale.US, "%.2f", average));
                context.write(key, result);
            }
        }
    }

    /**
     * Driver Main Method:
     * Configures and submits the MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar trustbank-p3.jar AvgTransactionAmountByType <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TrustBank - Problem 3: Avg Amount by Type (Boya Abhinay)");

        job.setJarByClass(AvgTransactionAmountByType.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
