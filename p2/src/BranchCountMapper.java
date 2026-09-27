package p2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BranchCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final Text branch = new Text();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.trim().isEmpty()) {
            return;
        }

        String[] fields = line.split(",", -1);

        // txnId, accountId, branch, amount, type, timestamp
        if (fields.length < 6) {
            return;
        }

        branch.set(fields[2].trim());

        context.write(branch, ONE);
    }
}