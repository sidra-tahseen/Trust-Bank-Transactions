# TrustBank BDA Case Study

## Team 4 – Banking Transaction Analytics

This project implements Big Data Analytics techniques using Hadoop MapReduce, YARN, Hive and Pig.

---

## Dataset

`transactions.csv`

### Attributes

- txnId
- accountId
- branch
- amount
- type
- timestamp

The dataset contains **4,347 transactions**.

The dataset is stored in HDFS at:

`/trustbank/input/transactions.csv`

---

## Business Problems

1. Total transaction amount processed by each branch.
2. High-value transactions and branch with the highest number of high-value transactions.
3. Average transaction amount by transaction type.
4. Weekend vs weekday transaction volume by branch.
5. Day-wise trend of flagged/high-value transactions.
6. Reimplementation of Problem 2 using Hive and Pig.

---

## Technologies

- Hadoop
- HDFS
- MapReduce
- YARN
- Hive
- Pig
- Java
- Docker

---

## MapReduce Technique

**Pipelining MapReduce Jobs**

Problem 2 uses a two-stage MapReduce pipeline:

**Transactions Dataset → Job 1: Filter High-Value Transactions → Intermediate HDFS Output → Job 2: Count High-Value Transactions by Branch → Final Branch-wise Counts**

The two jobs are chained using Hadoop `JobControl`, ensuring that Job 2 starts only after Job 1 completes successfully.

---

## Team

| S.No | HT No | Name | Problem Allocated |
| :---: | :---: | :--- | :--- |
| 1 | 160124733309 | SIDRA TAHSEEN | Problem 2 + 6: High-value transactions & Hive/Pig |
| 2 | 160124733293 | MANANYA YEGGE | Problem 5: Day-wise trend in flagged transactions |
| 3 | 160124733315 | BOYA ABHINAY KUMAR | Problem 3: Average transaction amount by type |
| 4 | 160124733327 | LENKAPOTHULA NITHISH KUMAR GOUD | Problem 4: Weekend vs weekday transaction volume |
| 5 | 160124733285 | DODLA AKSHITHA |  Problem 1: Total transaction amount by branch |

---

# Problem 1 – Total Transaction Amount by Branch

## Objective

Calculate the total transaction amount processed by each branch.

## Approach

The MapReduce implementation consists of:

- **Mapper:** Extracts the branch and transaction amount.
- **Reducer:** Groups transactions by branch and calculates the total amount.

## Verified Output

| Branch | Total Transaction Amount |
|---|---:|
| Bangalore-Koramangala | 8,241,305.66 |
| Bangalore-Whitefield | 10,168,148.87 |
| Chennai-TNagar | 6,070,048.81 |
| Delhi-CP | 9,031,742.09 |
| Delhi-Saket | 7,311,757.73 |
| Hyderabad-Banjara | 9,107,636.49 |
| Hyderabad-Gachibowli | 7,260,372.66 |
| Mumbai-Andheri | 6,545,737.35 |
| Mumbai-Bandra | 6,465,969.89 |
| Pune-Kothrud | 5,926,916.09 |

The MapReduce job processed the complete dataset and produced results for all 10 branches.

---

# Problem 2 – High-Value Transactions by Branch

## Objective

Determine the number of transactions exceeding a high-value threshold and identify the branch with the highest number of high-value transactions.

### High-Value Threshold

**₹50,000**

A transaction is considered high-value when `amount > 50000`.

## Two-Stage MapReduce Pipeline

### Job 1 – Filter High-Value Transactions

The Mapper reads the transaction records and filters transactions whose amount is greater than ₹50,000.

The filtered transactions are written to an intermediate HDFS directory.

### Job 2 – Branch-wise Count

The second MapReduce job reads the intermediate output, groups transactions by branch, and counts the high-value transactions for each branch.

The jobs are connected using Hadoop `JobControl`.

## Verified Output

| Branch | High-Value Transaction Count |
|---|---:|
| Bangalore-Whitefield | 17 |
| Delhi-Saket | 14 |
| Delhi-CP | 13 |
| Bangalore-Koramangala | 12 |
| Hyderabad-Gachibowli | 10 |
| Hyderabad-Banjara | 10 |
| Chennai-TNagar | 9 |
| Pune-Kothrud | 8 |
| Mumbai-Andheri | 8 |
| Mumbai-Bandra | 7 |

**Total high-value transactions: 108**

---

# Problem 3 – Average Transaction Amount by Type

## Objective

Calculate the average transaction amount for each transaction type:

- Deposit
- Withdrawal
- Transfer

## Approach

The MapReduce implementation:

- **Mapper:** Emits transaction type and amount.
- **Reducer:** Aggregates the transaction amounts and calculates the average for each type.

## Verified Output

| Transaction Type | Average Transaction Amount |
|---|---:|
| Deposit | 16,621.64 |
| Transfer | 18,377.92 |
| Withdrawal | 17,640.68 |

Detailed implementation is available in the [P3 README](p3/README.md).

---

# Problem 4 – Weekend vs Weekday Transaction Volume

## Objective

Determine transaction volumes for each branch separately for:

- Weekdays
- Weekends

## Approach

The Mapper processes the transaction timestamp and determines whether the transaction occurred on a weekday or weekend.

A Combiner performs local aggregation before the shuffle phase, while the Reducer calculates the final branch-wise volumes.

## Verified Results

- **Highest weekend transaction volume:** Hyderabad-Banjara – 106
- **Highest weekday transaction volume:** Hyderabad-Banjara – 469
- **Highest weekend share:** Chennai-TNagar – 22.69%

---

# Problem 5 – Day-wise Trend of High-Value Transactions

## Objective

Determine the daily trend of transactions exceeding the high-value threshold of ₹50,000.

## Approach

The Mapper:

1. Reads each transaction.
2. Checks whether the transaction amount exceeds ₹50,000.
3. Extracts the transaction date.
4. Emits the date for high-value transactions.

The Reducer counts the high-value transactions for each date.

## Verified Results

- **Total high-value transactions:** 108
- **Peak daily high-value transaction count:** 7
- **Peak date:** 2026-06-25

The output provides the complete day-wise distribution of flagged transactions across the reporting period.

---

# Problem 6 – Hive and Pig Implementation

## Objective

Reimplement **Problem 2** using both Hive and Pig on the same HDFS dataset.

Both implementations use the same high-value threshold:

**amount > ₹50,000**

Both produced results consistent with the MapReduce implementation.

---

## Hive Implementation

Hive uses a custom UDF:

`isHighValue(amount, threshold)`

The UDF returns `true` when the transaction amount is greater than the supplied threshold.

### Hive Workflow

1. Register the `isHighValue` UDF.
2. Create the temporary Hive function.
3. Read the transactions table.
4. Filter transactions using the UDF.
5. Group the filtered transactions by branch.
6. Count the transactions in each branch.
7. Order the results by count.

### Hive Query

`ADD JAR /root/p6/ishighvalue.jar;`

`CREATE TEMPORARY FUNCTION isHighValue AS 'udf.IsHighValue';`

`USE trustbank;`

`SET hive.execution.engine=mr;`

`SELECT branch, COUNT(*) AS high_value_count FROM transactions WHERE isHighValue(amount, 50000.0) GROUP BY branch ORDER BY count(*) DESC;`

### Hive Result

The Hive implementation produced the same 10 branch-wise counts as Problem 2.

**Observed Hive execution time: 45.295 seconds.**

---

## Pig Implementation

The Pig implementation performs the same analysis using Pig Latin.

### Pig Workflow

1. Load the transactions from HDFS using `PigStorage`.
2. Remove the CSV header.
3. Filter transactions where `amount > 50000`.
4. Group the filtered transactions by branch.
5. Count the transactions in each branch.
6. Order the results by count.
7. Store the result in HDFS.

### Pig Script

    transactions = LOAD 'hdfs:///trustbank/input/transactions.csv'
    USING PigStorage(',')
    AS (txnId:chararray, accountId:chararray, branch:chararray, amount:double, type:chararray, txn_timestamp:chararray);

    data = FILTER transactions BY txnId != 'txnId';

    high_value = FILTER data BY amount > 50000.0;

    grouped = GROUP high_value BY branch;

    counts = FOREACH grouped GENERATE
        group AS branch,
        COUNT(high_value) AS high_value_count;

    ordered = ORDER counts BY high_value_count DESC;

    STORE ordered INTO '/trustbank/p6/pig_branch_counts'
    USING PigStorage('\t');

The Pig YARN application completed successfully with `FinalApplicationStatus=SUCCEEDED`.

The generated HDFS output contained the same branch-wise results as Hive and MapReduce.

---

# Hive vs Pig Comparison

| Aspect | Hive | Pig |
|---|---|---|
| Development effort | Requires SQL and registration of a custom UDF | Requires a short Pig Latin data-flow script |
| UDF requirement | Uses custom `isHighValue` UDF | No custom UDF required for the high-value filter |
| Readability | SQL-like and familiar to users with SQL knowledge | Concise and naturally represents a data-flow pipeline |
| Filtering | `WHERE isHighValue(amount, 50000.0)` | `FILTER data BY amount > 50000.0` |
| Grouping | SQL `GROUP BY branch` | Pig `GROUP high_value BY branch` |
| Counting | `COUNT(*)` | `COUNT(high_value)` |
| Output | 10 branch counts | Same 10 branch counts |
| Total high-value transactions | 108 | 108 |
| Execution time | 45.295 seconds observed | YARN application completed successfully |

### Execution Note for Pig

The Pig YARN application successfully completed. After completion, the Pig client repeatedly attempted to connect to the JobHistory Server at `0.0.0.0:10020`.

This affected the Pig client's termination/display but did not affect the completed YARN application or the generated HDFS output.

Therefore, an exact end-to-end Pig client execution time was not used for comparison.

## Recommendation

For this particular high-value transaction analysis, **Pig is preferred for development simplicity** because filtering, grouping, and counting can be expressed directly as a short sequence of Pig Latin operations without requiring a custom UDF.

Hive remains useful when a SQL-style interface is preferred or when the team is more comfortable with relational query syntax.

---

# Overall Results

| Problem | Verified Result |
|---|---|
| **P1** | Total transaction amount calculated for all 10 branches |
| **P2** | 108 high-value transactions; Bangalore-Whitefield has 17 |
| **P3** | Average transaction amount calculated for deposit, transfer and withdrawal |
| **P4** | Weekend and weekday transaction volumes calculated by branch |
| **P5** | 108 flagged transactions; peak daily count of 7 on 2026-06-25 |
| **P6 – Hive** | Same P2 result; 45.295 seconds observed |
| **P6 – Pig** | Same P2 result; YARN application successfully completed |

---

# Execution Environment

The MapReduce programs were executed on a Docker-based Hadoop cluster using:

- Hadoop 3.2.1
- Java 8
- HDFS
- YARN
- MapReduce
- NameNode
- DataNode
- ResourceManager
- NodeManager
- HistoryServer

Hive and Pig were executed against the same HDFS dataset so that their results could be directly compared with the MapReduce implementation.

---

# Repository Structure

    Trust Bank Transactions/
    │
    ├── dataset/
    │   └── transactions.csv
    │
    ├── p1/
    │
    ├── p2/
    │   └── src/
    │
    ├── p3/
    │
    ├── p4/
    │
    ├── p5/
    │
    ├── p6/
    │   ├── README.md
    │   └── src/
    │       ├── IsHighValue.java
    │       ├── p2_hive.sql
    │       └── p2_pig.pig
    │
    ├── .gitignore
    └── README.md

---

# Conclusion

The TrustBank transaction dataset was successfully processed using Hadoop MapReduce on YARN for five analytical problems.

Problem 2 was implemented as a chained two-stage MapReduce pipeline, separating the filtering of high-value transactions from branch-wise aggregation.

The same high-value transaction analysis was reimplemented using Hive and Pig on the same HDFS dataset. Both implementations produced results consistent with the MapReduce implementation, with **108 high-value transactions** identified across the dataset.

The project demonstrates the use of Hadoop MapReduce, HDFS, YARN, Hive, Pig, custom Hive UDFs, and pipelined MapReduce jobs for banking transaction analytics.
