package trustbank.problem4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner for Problem 4: Weekend vs Weekday Transaction Volume by Branch.
 * Performs in-memory local aggregation on each Mapper node before the shuffle phase,
 * significantly reducing network traffic and disk spill.
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class WeekendWeekdayCombiner extends Reducer<Text, VolumeWritable, Text, VolumeWritable> {

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
