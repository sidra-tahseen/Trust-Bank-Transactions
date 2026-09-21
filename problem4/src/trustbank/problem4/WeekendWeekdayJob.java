package trustbank.problem4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.DayOfWeek;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * All-in-One Standalone MapReduce Solution for Problem 4:
 * "Which branch shows the highest transaction volume on weekends versus weekdays?"
 *
 * Contains VolumeWritable, MapClass, CombineClass, ReduceClass, and Driver logic in a single file.
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class WeekendWeekdayJob extends Configured implements Tool {

    // =========================================================================
    // 1. CUSTOM WRITABLE: Stores Weekday and Weekend Counts
    // =========================================================================
    public static class VolumeWritable implements Writable {
        private long weekdayCount;
        private long weekendCount;

        public VolumeWritable() {
            this(0, 0);
        }

        public VolumeWritable(long weekdayCount, long weekendCount) {
            this.weekdayCount = weekdayCount;
            this.weekendCount = weekendCount;
        }

        public long getWeekdayCount() { return weekdayCount; }
        public void setWeekdayCount(long weekdayCount) { this.weekdayCount = weekdayCount; }

        public long getWeekendCount() { return weekendCount; }
        public void setWeekendCount(long weekendCount) { this.weekendCount = weekendCount; }

        public void set(long weekdayCount, long weekendCount) {
            this.weekdayCount = weekdayCount;
            this.weekendCount = weekendCount;
        }

        public long getTotalCount() {
            return weekdayCount + weekendCount;
        }

        public double getWeekendRatio() {
            long total = getTotalCount();
            return total == 0 ? 0.0 : ((double) weekendCount / total) * 100.0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(weekdayCount);
            out.writeLong(weekendCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.weekdayCount = in.readLong();
            this.weekendCount = in.readLong();
        }

        @Override
        public String toString() {
            return weekdayCount + "\t" + weekendCount;
        }
    }

    // =========================================================================
    // 2. MAPPER CLASS
    // =========================================================================
    public static class MapClass extends Mapper<LongWritable, Text, Text, VolumeWritable> {
        private final Text branchKey = new Text();
        private final VolumeWritable volume = new VolumeWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("txnId") || line.contains("accountId")) {
                return; // Skip header and empty lines
            }

            String[] fields = line.split(",");
            if (fields.length < 6) return;

            String branch = fields[2].trim();
            String timestampStr = fields[5].trim();

            if (branch.isEmpty() || timestampStr.isEmpty()) return;

            try {
                String datePart = timestampStr.length() >= 10 ? timestampStr.substring(0, 10) : timestampStr;
                LocalDate transactionDate = LocalDate.parse(datePart);
                DayOfWeek dayOfWeek = transactionDate.getDayOfWeek();

                boolean isWeekend = (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY);

                branchKey.set(branch);
                if (isWeekend) {
                    volume.set(0, 1);
                } else {
                    volume.set(1, 0);
                }
                context.write(branchKey, volume);
            } catch (Exception ignored) {
                // Malformed date/record
            }
        }
    }

    // =========================================================================
    // 3. COMBINER CLASS (Local in-memory pre-aggregation)
    // =========================================================================
    public static class CombineClass extends Reducer<Text, VolumeWritable, Text, VolumeWritable> {
        private final VolumeWritable combinedVolume = new VolumeWritable();

        @Override
        protected void reduce(Text key, Iterable<VolumeWritable> values, Context context)
                throws IOException, InterruptedException {
            long totalWeekday = 0;
            long totalWeekend = 0;

            for (VolumeWritable val : values) {
                totalWeekday += val.getWeekdayCount();
                totalWeekend += val.getWeekendCount();
            }

            combinedVolume.set(totalWeekday, totalWeekend);
            context.write(key, combinedVolume);
        }
    }

    // =========================================================================
    // 4. REDUCER CLASS (Global aggregation & business leader analysis)
    // =========================================================================
    public static class ReduceClass extends Reducer<Text, VolumeWritable, Text, Text> {
        private final Text resultVal = new Text();

        private String highestWeekendBranch = "";
        private long maxWeekendCount = -1;

        private String highestWeekdayBranch = "";
        private long maxWeekdayCount = -1;

        private String highestRatioBranch = "";
        private double maxWeekendRatio = -1.0;

        @Override
        protected void reduce(Text key, Iterable<VolumeWritable> values, Context context)
                throws IOException, InterruptedException {
            long totalWeekday = 0;
            long totalWeekend = 0;

            for (VolumeWritable val : values) {
                totalWeekday += val.getWeekdayCount();
                totalWeekend += val.getWeekendCount();
            }

            long totalCount = totalWeekday + totalWeekend;
            double weekendRatio = totalCount > 0 ? ((double) totalWeekend / totalCount) * 100.0 : 0.0;

            if (totalWeekend > maxWeekendCount) {
                maxWeekendCount = totalWeekend;
                highestWeekendBranch = key.toString();
            }

            if (totalWeekday > maxWeekdayCount) {
                maxWeekdayCount = totalWeekday;
                highestWeekdayBranch = key.toString();
            }

            if (weekendRatio > maxWeekendRatio) {
                maxWeekendRatio = weekendRatio;
                highestRatioBranch = key.toString();
            }

            String formattedOutput = String.format(
                Locale.US,
                "Weekday Volume: %-5d | Weekend Volume: %-5d | Total Volume: %-5d | Weekend Share: %6.2f%%",
                totalWeekday, totalWeekend, totalCount, weekendRatio
            );

            resultVal.set(formattedOutput);
            context.write(key, resultVal);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text(""));
            context.write(new Text("=========================================================================================================="), new Text(""));
            context.write(new Text("PROBLEM 4 ANALYTICAL VERDICT & SUMMARY:"), new Text(""));
            context.write(new Text("=========================================================================================================="), new Text(""));
            context.write(
                new Text("1. HIGHEST WEEKEND TRANSACTION VOLUME"),
                new Text(String.format(Locale.US, ": %s with %d transactions", highestWeekendBranch, maxWeekendCount))
            );
            context.write(
                new Text("2. HIGHEST WEEKDAY TRANSACTION VOLUME"),
                new Text(String.format(Locale.US, ": %s with %d transactions", highestWeekdayBranch, maxWeekdayCount))
            );
            context.write(
                new Text("3. HIGHEST WEEKEND PROPORTION / SHARE"),
                new Text(String.format(Locale.US, ": %s with %.2f%% weekend transactions", highestRatioBranch, maxWeekendRatio))
            );
            context.write(new Text("=========================================================================================================="), new Text(""));
        }
    }

    // =========================================================================
    // 5. DRIVER RUNNER
    // =========================================================================
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WeekendWeekdayJob <input_path> <output_path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TrustBank - Problem 4 Weekend vs Weekday Transaction Volume");
        job.setJarByClass(WeekendWeekdayJob.class);

        job.setMapperClass(MapClass.class);
        job.setCombinerClass(CombineClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VolumeWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WeekendWeekdayJob(), args);
        System.exit(exitCode);
    }
}
