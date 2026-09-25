package trustbank.problem5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class HighValueTrendMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final double THRESHOLD = 50000.0;

    private final Text date = new Text();
    private final IntWritable one = new IntWritable(1);

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

                String timestamp = fields[5];

                // Extract YYYY-MM-DD
                String transactionDate = timestamp.split(" ")[0];

                date.set(transactionDate);

                // Emit: date -> 1
                context.write(date, one);
            }

        } catch (NumberFormatException e) {
            // Ignore invalid amount values
        }
    }
}
