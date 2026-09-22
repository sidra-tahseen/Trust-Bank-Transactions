ADD JAR /root/p6/ishighvalue.jar;
CREATE TEMPORARY FUNCTION isHighValue AS 'udf.IsHighValue';
USE trustbank;
SET hive.execution.engine=mr;
SELECT branch, COUNT(*) AS high_value_count FROM transactions WHERE isHighValue(amount, 50000.0) GROUP BY branch ORDER BY count(*) DESC;
