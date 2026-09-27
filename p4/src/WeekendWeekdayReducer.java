package trustbank.problem4;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Problem 4: Weekend vs Weekday Transaction Volume by Branch.
 * Aggregates weekday and weekend volumes per branch, computes ratios,
 * and identifies which branch exhibits the highest volumes and weekend share.
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class WeekendWeekdayReducer extends Reducer<Text, VolumeWritable, Text, Text> {

    private final Text resultVal = new Text();

    // Tracking variables for analytical business leaders
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

        // Check and update highest weekend volume leader
        if (totalWeekend > maxWeekendCount) {
            maxWeekendCount = totalWeekend;
            highestWeekendBranch = key.toString();
        }

        // Check and update highest weekday volume leader
        if (totalWeekday > maxWeekdayCount) {
            maxWeekdayCount = totalWeekday;
            highestWeekdayBranch = key.toString();
        }

        // Check and update highest weekend percentage leader
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
