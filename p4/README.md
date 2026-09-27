# Problem 4: Weekend vs Weekday Transaction Volume by Branch

**Team 4 – TrustBank Banking Transaction Analytics**  
**Member 4:** LENKAPOTHULA NITHISH KUMAR GOUD (HT No: 160124733327)

---

## Directory Structure

```
problem4/
├── src/
│   └── trustbank/
│       └── problem4/
│           ├── VolumeWritable.java          # Custom Writable for weekday and weekend counters
│           ├── WeekendWeekdayMapper.java    # Mapper classifying records into weekday/weekend
│           ├── WeekendWeekdayCombiner.java  # Combiner performing node-local pre-aggregation
│           ├── WeekendWeekdayReducer.java   # Reducer computing branch volume and identifying leaders
│           ├── WeekendWeekdayDriver.java    # Driver configuring Job and YARN parameters
│           └── WeekendWeekdayJob.java       # Standalone all-in-one class with static inner classes
├── bin/                                     # Compiled Java bytecode (.class files)
├── problem4.jar                             # Precompiled executable MapReduce JAR
├── compile.bat                              # Batch script to compile sources and build JAR
├── run.bat                                  # 1-Click script to run on local Windows Hadoop
├── run_docker.bat                           # 1-Click script to run on Docker Hadoop Cluster (HDFS + YARN)
├── output/
│   ├── part-r-00000                         # Final MapReduce output file
│   ├── job_execution_log.txt                # Full Hadoop execution stdout/stderr log
│   └── analytical_verdict.txt               # Plain-text business findings and analysis
├── EXPLANATION.md                           # Comprehensive academic documentation & code explanation
└── README.md                                # This quick start guide
```

---

## Quick Start Commands

### 1. Compile & Package
```bat
compile.bat
```

### 2. Run MapReduce Job (Local Mode)
```bat
run.bat
```

### 3. Run MapReduce Job on Docker Cluster (HDFS + YARN)
```bat
run_docker.bat
```

### 4. Run with Hadoop CLI Manually
```bat
hadoop jar problem4.jar trustbank.problem4.WeekendWeekdayDriver ..\dataset\transactions.csv output
```

### 5. Run on Hadoop Distributed Cluster (HDFS / YARN)
```bash
hdfs dfs -put ../dataset/transactions.csv /trustbank/dataset/
hadoop jar problem4.jar trustbank.problem4.WeekendWeekdayDriver /trustbank/dataset/transactions.csv /trustbank/output
hdfs dfs -cat /trustbank/output/part-r-00000
```

---

## Key Results Summary

- **Highest Weekend Transaction Volume**: **Hyderabad-Banjara** (106 transactions)
- **Highest Weekday Transaction Volume**: **Hyderabad-Banjara** (469 transactions)
- **Highest Weekend Share / Ratio**: **Chennai-TNagar** (22.69% of all transactions)
- **Total Records Processed**: 4,347 transactions across 10 branches (0 invalid/malformed records)

For detailed code architecture, data flow diagrams, and banking insights, see [EXPLANATION.md](EXPLANATION.md).
