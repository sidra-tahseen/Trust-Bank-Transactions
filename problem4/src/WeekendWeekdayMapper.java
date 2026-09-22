package trustbank.problem4;

import java.io.IOException;
import java.time.LocalDate;
import java.time.DayOfWeek;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Problem 4: Weekend vs Weekday Transaction Volume by Branch.
 * Parses transaction records, extracts branch and timestamp, determines if
 * transaction occurred on a weekend or weekday, and emits (branch, VolumeWritable).
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class WeekendWeekdayMapper extends Mapper<LongWritable, Text, Text, VolumeWritable> {

    private final Text branchKey = new Text();
    private final VolumeWritable volume = new VolumeWritable();

    public enum TransactionCounters {
        TOTAL_RECORDS,
        VALID_RECORDS,
        HEADER_SKIPPED,
        MALFORMED_RECORDS,
        WEEKEND_COUNT,
        WEEKDAY_COUNT
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        context.getCounter(TransactionCounters.TOTAL_RECORDS).increment(1);

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header line
        if (line.startsWith("txnId") || line.contains("accountId") || line.contains("branch")) {
            context.getCounter(TransactionCounters.HEADER_SKIPPED).increment(1);
            return;
        }

        String[] fields = line.split(",");
        // Schema: txnId, accountId, branch, amount, type, timestamp
        if (fields.length < 6) {
            context.getCounter(TransactionCounters.MALFORMED_RECORDS).increment(1);
            return;
        }

        String branch = fields[2].trim();
        String timestampStr = fields[5].trim();

        if (branch.isEmpty() || timestampStr.isEmpty()) {
            context.getCounter(TransactionCounters.MALFORMED_RECORDS).increment(1);
            return;
        }

        try {
            // Extract the date part (yyyy-MM-dd)
            String datePart = timestampStr.length() >= 10 ? timestampStr.substring(0, 10) : timestampStr;
            LocalDate transactionDate = LocalDate.parse(datePart);
            DayOfWeek dayOfWeek = transactionDate.getDayOfWeek();

            boolean isWeekend = (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY);

            branchKey.set(branch);

            if (isWeekend) {
                volume.set(0, 1);
                context.getCounter(TransactionCounters.WEEKEND_COUNT).increment(1);
            } else {
                volume.set(1, 0);
                context.getCounter(TransactionCounters.WEEKDAY_COUNT).increment(1);
            }

            context.getCounter(TransactionCounters.VALID_RECORDS).increment(1);
            context.write(branchKey, volume);

        } catch (Exception e) {
            context.getCounter(TransactionCounters.MALFORMED_RECORDS).increment(1);
        }
    }
}
