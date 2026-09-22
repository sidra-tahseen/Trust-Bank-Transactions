package p2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class HighValueMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final double THRESHOLD = 50000.0;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip CSV header
        if (line.startsWith("txnId,")) {
            return;
        }

        String[] fields = line.split(",", -1);

        // Expected format:
        // txnId, accountId, branch, amount, type, timestamp

        if (fields.length < 6) {
            return;
        }

        try {
            double amount = Double.parseDouble(fields[3]);

            if (amount > THRESHOLD) {
                context.write(NullWritable.get(), value);
            }

        } catch (NumberFormatException e) {
            // Ignore invalid amount values
        }
    }
}