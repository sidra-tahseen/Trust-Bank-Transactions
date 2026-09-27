package udf;
import org.apache.hadoop.hive.ql.exec.UDF;
public class IsHighValue extends UDF {
    public Boolean evaluate(Double amount, Double threshold) {
        if (amount == null || threshold == null) return false;
        return amount > threshold;
    }
}
