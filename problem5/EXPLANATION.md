# Problem 5 – Day-wise High Value Transaction Trend

## 1. Problem Statement

The objective of Problem 5 is to find the day-wise trend of high-value transactions using Hadoop MapReduce.

A transaction is considered high-value when its amount is greater than `50,000`.

The program reads bank transaction records, filters high-value transactions, extracts the transaction date, and counts the number of high-value transactions for each day.

---

## 2. Input Format

The input CSV file contains the following fields:

```text
txnId,accountId,branch,amount,type,timestamp
```

Example:

```text
TXN001,ACC101,Hyderabad,75000,DEBIT,2026-01-15 10:30:00
```

The important fields used by the program are:

- `fields[3]` → Transaction amount
- `fields[5]` → Transaction timestamp

---

## 3. Objective

The program performs the following operations:

1. Read each transaction.
2. Ignore the CSV header.
3. Extract the transaction amount.
4. Check whether the amount is greater than `50,000`.
5. Extract only the date from the timestamp.
6. Emit the date with a value of `1`.
7. Group all values having the same date.
8. Add the values for each date.
9. Produce the total number of high-value transactions for each day.

The overall flow is:

```text
Input CSV
    ↓
Mapper
    ↓
Filter amount > 50,000
    ↓
Extract date
    ↓
date → 1
    ↓
Shuffle and Sort
    ↓
Group by date
    ↓
Reducer
    ↓
Sum values
    ↓
date → total high-value transactions
```

---

# 4. MapReduce Components

Problem 5 contains three Java classes:

```text
HighValueTrendDriver.java
HighValueTrendMapper.java
HighValueTrendReducer.java
```

## 4.1 Driver

`HighValueTrendDriver` configures and submits the Hadoop MapReduce job.

## 4.2 Mapper

`HighValueTrendMapper` reads each transaction and identifies high-value transactions.

## 4.3 Reducer

`HighValueTrendReducer` counts the high-value transactions for each date.

---

# 5. Mapper Explanation

The Mapper is defined as:

```java
public class HighValueTrendMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>
```

The input consists of:

```text
LongWritable → Input key
Text → Transaction record
```

The output consists of:

```text
Text → Date
IntWritable → Count
```

---

## 5.1 High-Value Threshold

The Mapper defines:

```java
private static final double THRESHOLD = 50000.0;
```

Therefore, only transactions satisfying:

```text
amount > 50000
```

are considered high-value.

For example:

```text
60000 → High-value
75000 → High-value
100000 → High-value
```

Whereas:

```text
50000 → Not high-value
25000 → Not high-value
```

The condition is strictly greater than `50,000`.

---

## 5.2 Reading the Transaction

The Mapper converts the Hadoop `Text` value into a Java String:

```java
String line = value.toString();
```

For example:

```text
TXN001,ACC101,Hyderabad,75000,DEBIT,2026-01-15 10:30:00
```

---

## 5.3 Skipping the Header

The input CSV contains a header:

```text
txnId,accountId,branch,amount,type,timestamp
```

The Mapper checks:

```java
if (line.startsWith("txnId,")) {
    return;
}
```

If the line is the header, it is ignored.

This prevents the header from being processed as a transaction.

---

## 5.4 Splitting the Record

The transaction is split using commas:

```java
String[] fields = line.split(",", -1);
```

For example:

```text
TXN001,ACC101,Hyderabad,75000,DEBIT,2026-01-15 10:30:00
```

becomes:

```text
fields[0] = TXN001
fields[1] = ACC101
fields[2] = Hyderabad
fields[3] = 75000
fields[4] = DEBIT
fields[5] = 2026-01-15 10:30:00
```

---

## 5.5 Validating the Number of Fields

The Mapper checks:

```java
if (fields.length < 6) {
    return;
}
```

If a record does not contain at least six fields, it is ignored.

This prevents invalid records from causing errors when the program accesses `fields[3]` and `fields[5]`.

---

## 5.6 Extracting the Amount

The amount is stored at index `3`:

```java
double amount = Double.parseDouble(fields[3]);
```

For example:

```text
fields[3] = "75000"
```

is converted to:

```text
75000.0
```

---

## 5.7 Filtering High-Value Transactions

The Mapper checks:

```java
if (amount > THRESHOLD)
```

Since the threshold is `50000`, the condition becomes:

```text
amount > 50000
```

Only transactions satisfying this condition continue to the next step.

Transactions below or equal to `50,000` are ignored.

---

## 5.8 Extracting the Date

For a high-value transaction, the timestamp is obtained using:

```java
String timestamp = fields[5];
```

For example:

```text
2026-01-15 10:30:00
```

The program then extracts only the date:

```java
String transactionDate = timestamp.split(" ")[0];
```

The result is:

```text
2026-01-15
```

The time is removed because the objective is to calculate the number of transactions per day.

---

## 5.9 Mapper Output

The Mapper creates:

```java
private final IntWritable one = new IntWritable(1);
```

For every high-value transaction it emits:

```java
context.write(date, one);
```

Therefore, the Mapper output is:

```text
date → 1
```

For example:

```text
2026-01-15 → 1
2026-01-15 → 1
2026-01-16 → 1
```

Each `1` represents one high-value transaction.

---

# 6. Shuffle and Sort

Shuffle and Sort is automatically performed by Hadoop between the Mapper and Reducer.

Suppose the Mapper produces:

```text
2026-01-15 → 1
2026-01-15 → 1
2026-01-16 → 1
2026-01-15 → 1
```

Hadoop groups the values according to their keys:

```text
2026-01-15 → [1, 1, 1]
2026-01-16 → [1]
```

The Reducer then receives one date at a time along with all the values associated with that date.

---

# 7. Reducer Explanation

The Reducer is defined as:

```java
public class HighValueTrendReducer
        extends Reducer<Text, IntWritable, Text, IntWritable>
```

It receives:

```text
Date → List of counts
```

For example:

```text
2026-01-15 → [1,1,1]
```

---

## 7.1 Initializing the Sum

The Reducer starts with:

```java
int sum = 0;
```

This variable stores the total number of high-value transactions for the current date.

---

## 7.2 Adding the Values

The Reducer loops through all values:

```java
for (IntWritable value : values) {
    sum += value.get();
}
```

For:

```text
[1,1,1]
```

the calculation is:

```text
0 + 1 + 1 + 1 = 3
```

Therefore:

```text
2026-01-15 → 3
```

---

## 7.3 Writing the Final Result

The total is stored:

```java
total.set(sum);
```

and written using:

```java
context.write(key, total);
```

The final output is:

```text
date → number of high-value transactions
```

---

# 8. Driver Explanation

The Driver controls the complete MapReduce job.

It first checks that the input and output paths have been provided:

```java
if (args.length < 2)
```

The required arguments are:

```text
<input_path> <output_path>
```

---

## 8.1 Creating the Job

The Driver creates the Hadoop job:

```java
Job job = Job.getInstance(
    conf,
    "TrustBank - Day-wise High Value Transaction Trend (Problem 5)"
);
```

This creates a MapReduce job with the name:

```text
TrustBank - Day-wise High Value Transaction Trend (Problem 5)
```

---

## 8.2 Setting the Mapper and Reducer

The Driver specifies:

```java
job.setMapperClass(HighValueTrendMapper.class);
job.setReducerClass(HighValueTrendReducer.class);
```

Therefore:

```text
Mapper  → HighValueTrendMapper
Reducer → HighValueTrendReducer
```

---

## 8.3 Setting Map Output Types

The Mapper outputs:

```java
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
```

Therefore:

```text
Mapper Output:

Text → Date
IntWritable → 1
```

---

## 8.4 Setting Final Output Types

The final output types are:

```java
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
```

Therefore:

```text
Final Output:

Text → Date
IntWritable → Count
```

---

## 8.5 Input and Output Formats

The Driver uses:

```java
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
```

`TextInputFormat` allows Hadoop to read the input as text records.

`TextOutputFormat` writes the final results as text.

---

## 8.6 Setting Input and Output Paths

The paths are created using:

```java
Path inputPath = new Path(args[0]);
Path outputPath = new Path(args[1]);
```

The input path is registered using:

```java
FileInputFormat.addInputPath(job, inputPath);
```

The output path is registered using:

```java
FileOutputFormat.setOutputPath(job, outputPath);
```

---

## 8.7 Cleaning the Previous Output

The Driver checks whether the output directory already exists:

```java
if (fs.exists(outputPath)) {
    fs.delete(outputPath, true);
}
```

This is required because Hadoop normally does not allow a MapReduce job to write into an existing output directory.

The program therefore deletes the previous output before running the job again.

---

## 8.8 Running the Job

The job is submitted using:

```java
boolean success = job.waitForCompletion(true);
```

The program waits until the MapReduce job finishes.

If the job succeeds:

```text
Job completed successfully!
```

is displayed.

If it fails:

```text
Job execution failed!
```

is displayed.

---

# 9. Complete Data Flow Example

Suppose the input contains:

```text
T1,A1,HYD,60000,DEBIT,2026-01-01 10:00:00
T2,A2,HYD,20000,CREDIT,2026-01-01 11:00:00
T3,A3,DEL,80000,DEBIT,2026-01-01 12:00:00
T4,A4,MUM,90000,CREDIT,2026-01-02 09:00:00
```

### Mapper

Transaction 1:

```text
60000 > 50000
```

Output:

```text
2026-01-01 → 1
```

Transaction 2:

```text
20000 > 50000
```

False, so it is ignored.

Transaction 3:

```text
80000 > 50000
```

Output:

```text
2026-01-01 → 1
```

Transaction 4:

```text
90000 > 50000
```

Output:

```text
2026-01-02 → 1
```

### Shuffle and Sort

Hadoop groups the records:

```text
2026-01-01 → [1,1]
2026-01-02 → [1]
```

### Reducer

For `2026-01-01`:

```text
1 + 1 = 2
```

For `2026-01-02`:

```text
1 = 1
```

### Final Output

```text
2026-01-01    2
2026-01-02    1
```

---

# 10. How to Execute

The compiled JAR file is:

```text
problem5/problem5.jar
```

The general execution format is:

```bash
hadoop jar problem5/problem5.jar \
trustbank.problem5.HighValueTrendDriver \
<input_path> \
<output_path>
```

The two arguments are:

```text
input_path  → Location of transaction data
output_path → Location where Hadoop stores the result
```

The Driver automatically deletes the output directory if it already exists.

---

# 11. Output

The MapReduce output is stored in the output directory.

The main result file is:

After executing the Problem 5 MapReduce job, the output is stored in:

```text
problem5/output/part-r-00000
```

The output contains the date followed by the number of high-value transactions recorded on that date.

The program produced the following output:

```text
2026-06-03    1
2026-06-05    1
2026-06-06    2
2026-06-07    1
2026-06-08    2
2026-06-09    1
2026-06-11    1
2026-06-12    1
2026-06-13    1
2026-06-14    1
2026-06-15    1
2026-06-16    4
2026-06-17    1
2026-06-18    6
2026-06-20    1
2026-06-21    2
2026-06-22    2
2026-06-23    3
2026-06-24    1
2026-06-25    7
2026-06-26    4
2026-06-27    1
2026-06-28    1
2026-06-30    3
2026-07-01    1
2026-07-02    2
2026-07-03    1
2026-07-04    1
2026-07-05    3
2026-07-06    6
2026-07-07    3
2026-07-08    1
2026-07-09    2
2026-07-11    3
2026-07-12    1
2026-07-13    5
2026-07-14    1
2026-07-15    3
2026-07-16    3
2026-07-17    2
2026-07-18    1
2026-07-19    1
2026-07-20    2
2026-07-21    2
2026-07-22    2
2026-07-23    1
2026-07-24    1
2026-07-26    2
2026-07-27    3
2026-07-28    1
2026-07-29    4
2026-07-30    1
```

## Interpretation

Each line has the following format:

```text
Date    Number of high-value transactions
```

For example:

```text
2026-06-18    6
```

means that **6 transactions with an amount greater than 50,000 occurred on June 18, 2026**.

Similarly:

```text
2026-06-25    7
```

means that **7 high-value transactions occurred on June 25, 2026**.

The output is already grouped by date because Hadoop's Shuffle and Sort phase groups all Mapper outputs having the same date key.

The largest daily count visible in this output is:

```text
2026-06-25    7
```

This means 7 high-value transactions were recorded on June 25, 2026.

Other dates with relatively high counts include:

```text
2026-06-18    6
2026-07-06    6
2026-07-13    5
2026-06-16    4
2026-06-26    4
2026-07-29    4
```

The output demonstrates that the MapReduce program successfully filters high-value transactions and aggregates them on a day-wise basis.
# 12. Why This MapReduce Approach Is Useful

The program separates the processing into two stages.

The Mapper performs filtering and date extraction close to the input data.

The Reducer performs aggregation after Hadoop groups records by date.

This allows Hadoop to distribute the processing of large transaction datasets across multiple machines.

---

# 13. Key Points for Viva

### What is the purpose of Problem 5?

To calculate the number of high-value transactions for each day.

### What is a high-value transaction?

A transaction whose amount is greater than `50,000`.

### What does the Mapper output?

```text
date → 1
```

### Why does the Mapper output 1?

Each qualifying transaction represents one high-value transaction.

### What does Shuffle and Sort do?

It groups all values belonging to the same date.

### What does the Reducer do?

It sums all the `1`s for each date.

### What is the final output?

```text
date → number of high-value transactions
```

### Why is only the date extracted?

Because the problem requires a day-wise trend rather than a time-wise trend.

### What happens to invalid amounts?

Invalid numerical values are ignored using `NumberFormatException`.

### Why is the output directory deleted?

Hadoop does not normally allow a MapReduce job to write into an existing output directory.

---

# 14. One-Minute Explanation

Problem 5 uses Hadoop MapReduce to find the day-wise trend of high-value bank transactions. The Mapper reads each transaction and skips the CSV header. It extracts the transaction amount and checks whether it is greater than 50,000. If it is high-value, the Mapper extracts the date from the timestamp and emits the date as the key and `1` as the value. Hadoop then performs Shuffle and Sort and groups all the values belonging to the same date. The Reducer adds these values and produces the total number of high-value transactions for each day. The Driver configures the Mapper, Reducer, input path, output path, and submits the job to Hadoop.

---

# 15. Program Flow

```text
                 INPUT CSV
                     |
                     ↓
           HighValueTrendDriver
                     |
                     ↓
           HighValueTrendMapper
                     |
          ┌──────────┴──────────┐
          ↓                     ↓
    Amount > 50000?            No
          ↓                     ↓
         Yes                  Ignore
          |
          ↓
    Extract date
          |
          ↓
      date → 1
          |
          ↓
    Shuffle & Sort
          |
          ↓
   Group values by date
          |
          ↓
    HighValueTrendReducer
          |
          ↓
       Sum values
          |
          ↓
        OUTPUT
          |
          ↓
date → high-value transaction count
```
