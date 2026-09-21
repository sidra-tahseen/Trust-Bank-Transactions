# TrustBank BDA Case Study

## Team 4 – Banking Transaction Analytics

This project implements Big Data Analytics techniques using Hadoop MapReduce, YARN, Hive and Pig.

## Dataset

`transactions.csv`

### Attributes

- txnId
- accountId
- branch
- amount
- type
- timestamp

## Business Problems

1. Total transaction amount processed by each branch.
2. High-value transactions and branch with the highest number of high-value transactions.
3. Average transaction amount by transaction type.
4. Weekend vs weekday transaction volume by branch.
5. Day-wise trend of flagged/high-value transactions.

## Technologies

- Hadoop
- HDFS
- MapReduce
- YARN
- Hive
- Pig
- Java

## MapReduce Technique

Pipelining MapReduce Jobs

## Team

| S.No | HT No | Name | Problem Allocated |
| :---: | :---: | :--- | :--- |
| 1 | 160124733309 | SIDRA TAHSEEN | Problem 1: Total transaction amount by branch |
| 2 | 160124733293 | MANANYA YEGGE | Problem 2 + 6: High-value transactions & Hive/Pig |
| 3 | 160124733315 | BOYA ABHINAY KUMAR | **[Problem 3: Average transaction amount by type](p3/README.md)** |
| 4 | 160124733327 | LENKAPOTHULA NITHISH KUMAR GOUD | Problem 4: Weekend vs weekday transaction volume |
| 5 | 160124733285 | DODLA AKSHITHA | Problem 5: Day-wise trend in flagged transactions |

---

### Problem 3 Status: Completed
* **Author**: Boya Abhinay (HT No: `160124733315`)
* **Solution Path**: [`p3/`](p3/)
* **Full Documentation**: [`p3/EXPLANATION.md`](p3/EXPLANATION.md)
* **MapReduce Output**: [`p3/output/part-r-00000`](p3/output/part-r-00000)