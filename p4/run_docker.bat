@echo off

echo =====================================================================
echo Running Problem 4 MapReduce on Docker Hadoop Cluster (HDFS + YARN)
echo Author: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
echo =====================================================================

echo [1/4] Copying files to Docker NameNode container...
docker cp problem4.jar namenode:/problem4.jar
docker cp ..\dataset\transactions.csv namenode:/transactions.csv

echo [2/4] Uploading dataset to HDFS (/trustbank/dataset/)...
docker exec namenode hdfs dfs -mkdir -p /trustbank/dataset
docker exec namenode hdfs dfs -put -f /transactions.csv /trustbank/dataset/transactions.csv

echo [3/4] Submitting MapReduce Job to YARN on Docker cluster...
docker exec namenode hadoop jar /problem4.jar trustbank.problem4.WeekendWeekdayDriver /trustbank/dataset/transactions.csv /trustbank/output

echo.
echo =====================================================================
echo [4/4] HDFS OUTPUT RESULTS (from /trustbank/output/part-r-00000):
echo =====================================================================
docker exec namenode hdfs dfs -cat /trustbank/output/part-r-00000

echo =====================================================================
echo You can view the live YARN dashboard at: http://localhost:8088
echo You can view the HDFS NameNode at:        http://localhost:9870
echo =====================================================================
