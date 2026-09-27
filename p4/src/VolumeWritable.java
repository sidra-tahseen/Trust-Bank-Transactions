package trustbank.problem4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable to store weekday and weekend transaction counts.
 * Enables combining and reducing multi-dimensional volume counters in a single pass.
 *
 * @author Member 4: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
 */
public class VolumeWritable implements Writable {

    private long weekdayCount;
    private long weekendCount;

    public VolumeWritable() {
        this(0, 0);
    }

    public VolumeWritable(long weekdayCount, long weekendCount) {
        this.weekdayCount = weekdayCount;
        this.weekendCount = weekendCount;
    }

    public long getWeekdayCount() {
        return weekdayCount;
    }

    public void setWeekdayCount(long weekdayCount) {
        this.weekdayCount = weekdayCount;
    }

    public long getWeekendCount() {
        return weekendCount;
    }

    public void setWeekendCount(long weekendCount) {
        this.weekendCount = weekendCount;
    }

    public void set(long weekdayCount, long weekendCount) {
        this.weekdayCount = weekdayCount;
        this.weekendCount = weekendCount;
    }

    public void add(VolumeWritable other) {
        this.weekdayCount += other.weekdayCount;
        this.weekendCount += other.weekendCount;
    }

    public long getTotalCount() {
        return weekdayCount + weekendCount;
    }

    public double getWeekendRatio() {
        long total = getTotalCount();
        return total == 0 ? 0.0 : ((double) weekendCount / total) * 100.0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(weekdayCount);
        out.writeLong(weekendCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.weekdayCount = in.readLong();
        this.weekendCount = in.readLong();
    }

    @Override
    public String toString() {
        return weekdayCount + "\t" + weekendCount;
    }
}
