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
