package trustbank.problem5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighValueTrendReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable total = new IntWritable();

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        total.set(sum);

        // Output: date -> number of high-value transactions
        context.write(key, total);
    }
}
