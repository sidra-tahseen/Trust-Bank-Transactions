import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BranchTotalMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text branch = new Text();
    private DoubleWritable amount = new DoubleWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("txnId")) {
            return;
        }

        String[] data = line.split(",");

        if (data.length >= 6) {
            String branchName = data[2].trim();
            double transactionAmount =
                    Double.parseDouble(data[3].trim());

            branch.set(branchName);
            amount.set(transactionAmount);

            context.write(branch, amount);
        }
    }
}