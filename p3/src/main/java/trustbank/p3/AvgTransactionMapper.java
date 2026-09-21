package trustbank.p3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * TrustBank Analytics - Problem 3
 * Team Member: Boya Abhinay (Member 3) | HT No: 160124733315
 *
 * Mapper:
 * Reads each record from transactions.csv:
 *   Schema: txnId,accountId,branch,amount,type,timestamp
 * Filters out the CSV header and emits:
 *   Key:   Text (transaction type: deposit / withdrawal / transfer)
 *   Value: DoubleWritable (transaction amount)
 */
public class AvgTransactionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text transactionType = new Text();
    private final DoubleWritable transactionAmount = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header line
        if (line.startsWith("txnId") || line.toLowerCase().contains("accountid")) {
            return;
        }

        // Split CSV row by comma
        String[] tokens = line.split(",");

        // Schema requires at least 5 columns: txnId, accountId, branch, amount, type, [timestamp]
        if (tokens.length >= 5) {
            String amountStr = tokens[3].trim();
            String typeStr = tokens[4].trim().toLowerCase();

            try {
                double amount = Double.parseDouble(amountStr);
                transactionType.set(typeStr);
                transactionAmount.set(amount);
                context.write(transactionType, transactionAmount);
            } catch (NumberFormatException e) {
                // Log and skip invalid numeric record
                context.getCounter("TrustBank_P3", "MALFORMED_AMOUNT_RECORDS").increment(1);
            }
        } else {
            // Count malformed records with insufficient columns
            context.getCounter("TrustBank_P3", "INSUFFICIENT_COLUMNS").increment(1);
        }
    }
}
