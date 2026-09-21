package trustbank.p3;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TrustBank Analytics - Problem 3
 * Team Member: Boya Abhinay (Member 3) | HT No: 160124733315
 *
 * Reducer:
 * Receives:
 *   Key:    Text (transaction type: deposit / withdrawal / transfer)
 *   Values: Iterable<DoubleWritable> (list of transaction amounts)
 * Computes:
 *   totalAmount = sum of all transaction amounts for this type
 *   count       = total number of transactions for this type
 *   average     = totalAmount / count
 * Emits:
 *   Key:    Text (transaction type)
 *   Value:  Text (average transaction amount formatted to 2 decimal places)
 */
public class AvgTransactionReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalAmount = 0.0;
        long count = 0;

        for (DoubleWritable val : values) {
            totalAmount += val.get();
            count++;
        }

        if (count > 0) {
            double average = totalAmount / count;

            // Increment custom counters for auditing and verification
            String counterGroupName = "TrustBank_Summary";
            context.getCounter(counterGroupName, key.toString().toUpperCase() + "_COUNT").increment(count);

            // Format to 2 decimal places for financial accuracy
            result.set(String.format(Locale.US, "%.2f", average));
            context.write(key, result);
        }
    }
}
