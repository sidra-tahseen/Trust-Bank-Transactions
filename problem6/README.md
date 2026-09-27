# Problem 6 - Hive and Pig Comparison

## Problem

Re-implement Problem 2 using Hive and Pig on the same HDFS dataset. A transaction is considered high-value when its amount is greater than ?50,000.

## Hive Implementation

Hive uses a custom UDF named `isHighValue(amount, threshold)` to filter high-value transactions. The filtered records are grouped by branch and counted.

The Hive query produced the following result:

- Bangalore-Whitefield: 17
- Delhi-Saket: 14
- Delhi-CP: 13
- Bangalore-Koramangala: 12
- Hyderabad-Gachibowli: 10
- Hyderabad-Banjara: 10
- Chennai-TNagar: 9
- Pune-Kothrud: 8
- Mumbai-Andheri: 8
- Mumbai-Bandra: 7

Measured Hive execution time: **47.954 seconds**.

## Pig Implementation

Pig uses the `FILTER` operator to retain transactions above ?50,000, `GROUP` to group them by branch, `COUNT` to calculate the number of high-value transactions, and `ORDER` to sort the results.

Pig produced the same branch counts as Hive, with Bangalore-Whitefield having the highest count of 17.

The Pig MapReduce application completed successfully on YARN in approximately **20.2 seconds** from application submission to successful completion. After the application succeeded, the Pig client continued trying to connect to the JobHistory Server, so the client process took longer to terminate. This did not affect the generated HDFS output.

## Comparison

### Development Effort

Hive required writing a Java UDF in addition to the Hive SQL query. Pig required only a Pig Latin script using built-in relational operators. Therefore, for this particular problem, the Pig implementation involved less supporting code.

### Code Readability

Hive SQL is familiar and easy to understand for users comfortable with SQL. Pig Latin is also concise for filtering, grouping, counting, and ordering data. For this workflow, both implementations are readable, while Pig expresses the data-processing pipeline directly through its relational operators.

### Execution Time

| Technology | Observed execution |
|---|---:|
| Hive | 47.954 seconds |
| Pig | ~20.2 seconds* |

`*` Pig time represents the YARN application execution interval. The Pig client remained active afterward because of the JobHistory Server connection issue.

## Recommendation

For this particular high-value transaction analysis, **Pig is recommended** because the implementation is concise, requires no custom UDF for the filtering operation, and had a shorter observed YARN execution time in the test environment. Hive remains useful when the analysis is naturally expressed as SQL and when SQL familiarity is an important consideration.
