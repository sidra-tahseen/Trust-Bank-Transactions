# Problem 4: Weekend vs Weekday Transaction Volume by Branch
## TrustBank Banking Transaction Analytics — Big Data Analytics (BDA)

---

### Team & Contributor Information
- **Team Name**: Team 4 – TrustBank Transactions
- **Assigned Member**: **Member 4**
- **Student Name**: **LENKAPOTHULA NITHISH KUMAR GOUD**
- **Hall Ticket Number**: **160124733327**
- **Task Assignment**: **Problem 4 (Mapper + Reducer + Driver + Explanation + Output)**

---

## 1. Problem Statement & Business Objective

> **Problem 4**: *Which branch shows the highest transaction volume on weekends versus weekdays?*

### 1.1 Objective
In retail and commercial banking, transaction traffic varies substantially between standard working days (Monday through Friday) and weekends (Saturday and Sunday). Understanding these patterns allows TrustBank to:
1. **Optimize Branch Operations**: Adjust staffing levels, teller availability, and cash reserve allocations.
2. **Infrastructure Planning**: Schedule core banking software updates, data pipeline batches, and maintenance windows during low-traffic intervals.
3. **Risk & Fraud Surveillance**: Detect anomalies where off-hours or weekend volume spikes unexpectedly compared to historical branch baselines.

### 1.2 Target Deliverables
- **Custom Writable**: `VolumeWritable.java` for multi-metric encapsulation and bandwidth-efficient combiner aggregation.
- **Mapper**: `WeekendWeekdayMapper.java` for CSV parsing, timestamp conversion, calendar classification, and counter tracking.
- **Combiner**: `WeekendWeekdayCombiner.java` for local mini-reduction on cluster nodes.
- **Reducer**: `WeekendWeekdayReducer.java` for branch-level aggregation, weekend ratio calculation, and leader evaluation in `cleanup()`.
- **Driver**: `WeekendWeekdayDriver.java` implementing `Configured` and `Tool` for YARN cluster job execution.
- **All-in-One Job**: `WeekendWeekdayJob.java` for self-contained single-file execution.
- **Output & Execution Proof**: Actual generated `part-r-00000`, run logs, and analytical verdict.

---

## 2. Dataset Schema & Characteristics

The analytics job processes `dataset/transactions.csv`.

### 2.1 Attribute Schema
| Column Index | Field Name | Data Type | Description / Sample |
|:---:|:---|:---|:---|
| 0 | `txnId` | String | Unique transaction identifier (`TXN0000097`) |
| 1 | `accountId` | String | Account number (`ACC100714`) |
| 2 | `branch` | String | Bank branch identifier (`Pune-Kothrud`, `Hyderabad-Banjara`) |
| 3 | `amount` | Double | Monetary transaction amount (`1628.56`) |
| 4 | `type` | String | Transaction type (`deposit`, `withdrawal`, `transfer`) |
| 5 | `timestamp` | String | Transaction timestamp (`2026-06-01 00:18:30`) |

### 2.2 Dataset Statistics
- **Total Rows**: 4,348 (1 header line + 4,347 data records)
- **Date Range**: June 1, 2026 to July 30, 2026 (60 days across two full months)
- **Total Branches**: 10 distinct branch locations across 5 major metro regions (Bangalore, Chennai, Delhi, Hyderabad, Mumbai, Pune).

---

## 3. MapReduce Architecture & Pipeline

```
+----------------------------------------------------------------------------------------------------+
|                                      MAPREDUCE EXECUTION PIPELINE                                  |
+----------------------------------------------------------------------------------------------------+

   [ transactions.csv (HDFS / Input) ]
                 |
                 v
   +----------------------------------------------------+
   |               WeekendWeekdayMapper                 |
   |  - Tokenize CSV line                               |
   |  - Parse timestamp -> java.time.DayOfWeek          |
   |  - Emit: (Branch, VolumeWritable(wd=1, we=0)) OR   |
   |          (Branch, VolumeWritable(wd=0, we=1))      |
   +----------------------------------------------------+
                 |
                 | (Emits 4,347 Key-Value pairs)
                 v
   +----------------------------------------------------+
   |              WeekendWeekdayCombiner                |
   |  - Local in-memory reduction on Mapper node        |
   |  - Combines 4,347 records down to 10 records       |
   |  - Emits: (Branch, VolumeWritable(sumWd, sumWe))   |
   +----------------------------------------------------+
                 |
                 | (Shuffle & Sort across network: 346 bytes)
                 v
   +----------------------------------------------------+
   |               WeekendWeekdayReducer                |
   |  - Aggregates per branch totals                    |
   |  - Computes Total Volume & Weekend Share (%)       |
   |  - Identifies Max Weekend & Weekday Branches       |
   |  - Emits formatted row for each branch             |
   |  - In cleanup(): Emits Analytical Verdict          |
   +----------------------------------------------------+
                 |
                 v
   [ problem4/output/part-r-00000 (HDFS / Local Target) ]
```

### 3.1 Key-Value Transformations

| Stage | Input Key | Input Value | Output Key | Output Value |
|:---|:---|:---|:---|:---|
| **Mapper** | `LongWritable` (byte offset) | `Text` (CSV record) | `Text` (Branch) | `VolumeWritable` `(1,0)` or `(0,1)` |
| **Combiner** | `Text` (Branch) | `Iterable<VolumeWritable>` | `Text` (Branch) | `VolumeWritable` `(sumWd, sumWe)` |
| **Reducer** | `Text` (Branch) | `Iterable<VolumeWritable>` | `Text` (Branch / Header) | `Text` (Formatted Metrics / Verdict) |

---

## 4. Detailed Component Walkthrough

### 4.1 Custom Writable: `VolumeWritable.java`
MapReduce achieves peak performance when composite data is serialized efficiently using Hadoop's binary `Writable` interface rather than string parsing.

```java
public class VolumeWritable implements Writable {
    private long weekdayCount;
    private long weekendCount;

    public void write(DataOutput out) throws IOException {
        out.writeLong(weekdayCount);
        out.writeLong(weekendCount);
    }

    public void readFields(DataInput in) throws IOException {
        this.weekdayCount = in.readLong();
        this.weekendCount = in.readLong();
    }
}
```
- **Rationale**: Serializing two `long` primitives takes only 16 bytes, avoiding the memory overhead and string splitting costs of Text objects.

### 4.2 Mapper: `WeekendWeekdayMapper.java`
1. **Header Filtering**: Safely ignores the first line (`txnId,accountId,...`) and whitespace.
2. **Date Extraction**: Uses `java.time.LocalDate.parse()` on the `yyyy-MM-dd` date portion. `LocalDate` is thread-safe and immutable (unlike legacy `SimpleDateFormat`).
3. **Calendar Classification**:
   - `DayOfWeek.SATURDAY` and `DayOfWeek.SUNDAY` are tagged as **Weekend**.
   - `DayOfWeek.MONDAY` through `FRIDAY` are tagged as **Weekday**.
4. **Hadoop Counters**: Tracks `TOTAL_RECORDS`, `VALID_RECORDS`, `WEEKDAY_COUNT`, `WEEKEND_COUNT`, and `MALFORMED_RECORDS` in real-time.

### 4.3 Combiner: `WeekendWeekdayCombiner.java`
Acts as a localized pre-aggregation step on the Mapper node before network shuffle:
- **Efficiency Metric**: 4,347 mapper records were merged down to **only 10 combiner records** before leaving the node!
- **Bandwidth Reduction**: Network shuffle traffic dropped to only 346 bytes.

### 4.4 Reducer: `WeekendWeekdayReducer.java`
1. Iterates over `VolumeWritable` values for each branch to compute `totalWeekday`, `totalWeekend`, and `totalCount`.
2. Computes the **Weekend Share percentage**:
   $$\text{Weekend Share (\%)} = \left(\frac{\text{totalWeekend}}{\text{totalCount}}\right) \times 100$$
3. Tracks global maximums:
   - `maxWeekendCount` and `highestWeekendBranch`
   - `maxWeekdayCount` and `highestWeekdayBranch`
   - `maxWeekendRatio` and `highestRatioBranch`
4. Emits a clean, human-readable tabular line per branch.
5. In `cleanup(Context context)`, outputs the final analytical verdict summarizing the winners.

### 4.5 Driver: `WeekendWeekdayDriver.java`
- Configured with `ToolRunner` to accept standard Hadoop CLI options (`-D`, `-conf`, etc.).
- Enforces `setNumReduceTasks(1)` so a single reducer receives all branches, ensuring unified global sorting and comprehensive `cleanup()` analytical reporting.
- Checks and cleans existing output directories to prevent `FileAlreadyExistsException`.

---

## 5. Compilation and Execution Guide

### 5.1 Prerequisites
- Hadoop 3.x (`HADOOP_HOME` set)
- JDK 17 (`JAVA_HOME` set)

### 5.2 Compiling the Code
A helper batch script `compile.bat` is included in `problem4/`:
```bat
cd problem4
./compile.bat
```

Manual compilation commands:
```bash
# 1. Fetch Hadoop classpath
HADOOP_CP=$(hadoop classpath)

# 2. Compile Java classes targeting Java 17
javac -cp "$HADOOP_CP" -d bin src/trustbank/problem4/*.java

# 3. Create JAR archive
jar -cvf problem4.jar -C bin .
```

### 5.3 Ingesting Dataset into HDFS & Submitting Job to YARN
To run on a full distributed Hadoop cluster on YARN:
```bash
# 1. Create HDFS directories
hdfs dfs -mkdir -p /trustbank/dataset
hdfs dfs -mkdir -p /trustbank/problem4/output

# 2. Ingest the dataset into HDFS
hdfs dfs -put dataset/transactions.csv /trustbank/dataset/

# 3. Submit MapReduce job via YARN
hadoop jar problem4.jar trustbank.problem4.WeekendWeekdayDriver \
    /trustbank/dataset/transactions.csv \
    /trustbank/problem4/output

# 4. View results from HDFS
hdfs dfs -cat /trustbank/problem4/output/part-r-00000
```

### 5.4 Running in Standalone / Local Mode
```bat
cd problem4
call run.bat ..\dataset\transactions.csv output
```

---

## 6. Execution Results & Analysis

### 6.1 MapReduce Execution Job Counters
From `output/job_execution_log.txt`:
```
trustbank.problem4.WeekendWeekdayMapper$TransactionCounters
    TOTAL_RECORDS       = 4,348
    HEADER_SKIPPED      = 1
    VALID_RECORDS       = 4,347
    WEEKDAY_COUNT       = 3,545
    WEEKEND_COUNT       = 802
    MALFORMED_RECORDS   = 0

Map-Reduce Framework:
    Map input records      = 4,348
    Map output records     = 4,347
    Combine input records  = 4,347
    Combine output records = 10
    Reduce input records   = 10
    Reduce output records  = 18
```

### 6.2 Complete Branch Output (`part-r-00000`)
```
Bangalore-Koramangala	Weekday Volume: 411   | Weekend Volume: 95    | Total Volume: 506   | Weekend Share:  18.77%
Bangalore-Whitefield	Weekday Volume: 381   | Weekend Volume: 76    | Total Volume: 457   | Weekend Share:  16.63%
Chennai-TNagar	Weekday Volume: 276   | Weekend Volume: 81    | Total Volume: 357   | Weekend Share:  22.69%
Delhi-CP	Weekday Volume: 361   | Weekend Volume: 86    | Total Volume: 447   | Weekend Share:  19.24%
Delhi-Saket	Weekday Volume: 324   | Weekend Volume: 53    | Total Volume: 377   | Weekend Share:  14.06%
Hyderabad-Banjara	Weekday Volume: 469   | Weekend Volume: 106   | Total Volume: 575   | Weekend Share:  18.43%
Hyderabad-Gachibowli	Weekday Volume: 399   | Weekend Volume: 86    | Total Volume: 485   | Weekend Share:  17.73%
Mumbai-Andheri	Weekday Volume: 329   | Weekend Volume: 83    | Total Volume: 412   | Weekend Share:  20.15%
Mumbai-Bandra	Weekday Volume: 308   | Weekend Volume: 76    | Total Volume: 384   | Weekend Share:  19.79%
Pune-Kothrud	Weekday Volume: 287   | Weekend Volume: 60    | Total Volume: 347   | Weekend Share:  17.29%
```

### 6.3 Core Findings & Answers to Problem 4

| Metric | Winning Branch | Metric Value | Notable Runner-Up |
|:---|:---|:---|:---|
| **Highest Weekend Volume** | **Hyderabad-Banjara** | **106 transactions** | Bangalore-Koramangala (95) |
| **Highest Weekday Volume** | **Hyderabad-Banjara** | **469 transactions** | Bangalore-Koramangala (411) |
| **Highest Total Volume** | **Hyderabad-Banjara** | **575 transactions** | Bangalore-Koramangala (506) |
| **Highest Weekend Proportion / Share** | **Chennai-TNagar** | **22.69%** | Mumbai-Andheri (20.15%) |
| **Lowest Weekend Proportion** | **Delhi-Saket** | **14.06%** | Bangalore-Whitefield (16.63%) |

---

## 7. Banking & Business Insights

1. **Hyderabad-Banjara is TrustBank's Highest-Traffic Anchor Branch**:
   - Hyderabad-Banjara leads the bank across all categories with **469 weekday** and **106 weekend** transactions.
   - **Recommendation**: Ensure high-availability ATM hardware, redundant network links, and maximum teller staffing at the Banjara branch.

2. **Weekend Retail Activity in Chennai and Mumbai**:
   - While Chennai-TNagar has moderate overall volume (357), nearly **23% of its activity happens on weekends**, far exceeding the bank average of 18.45%.
   - Mumbai-Andheri (20.15%) and Mumbai-Bandra (19.79%) also display elevated weekend participation.
   - **Recommendation**: Target weekend-oriented promotional campaigns, merchant discounts, and increased ATM cash restocking specifically in Chennai and Mumbai retail corridors.

3. **Commercial Focus in Delhi-Saket and Bangalore-Whitefield**:
   - Delhi-Saket exhibits the lowest weekend ratio (14.06%), indicating that its customers are predominantly corporate or weekday B2B accounts.
   - **Recommendation**: Schedule branch-level system maintenance for Delhi-Saket during Saturdays and Sundays with minimal risk of customer disruption.

---

## 8. Summary Checklist of Member 4 Deliverables

- [x] **Mapper**: `WeekendWeekdayMapper.java` (implemented and verified)
- [x] **Combiner**: `WeekendWeekdayCombiner.java` (implemented and verified)
- [x] **Reducer**: `WeekendWeekdayReducer.java` (implemented and verified)
- [x] **Driver**: `WeekendWeekdayDriver.java` (implemented and verified)
- [x] **All-in-One Job**: `WeekendWeekdayJob.java` (implemented and verified)
- [x] **Build Scripts**: `compile.bat` and `run.bat` (tested and working)
- [x] **Compiled Artifact**: `problem4.jar` (packaged and validated)
- [x] **Execution Logs & Output**: `output/part-r-00000`, `job_execution_log.txt`, `analytical_verdict.txt`
- [x] **Detailed Academic Explanation**: `EXPLANATION.md`
