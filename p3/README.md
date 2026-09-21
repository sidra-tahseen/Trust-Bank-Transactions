# TrustBank Analytics — Problem 3 (P3)

## Member 3: Boya Abhinay (HT No: 160124733315)
**Problem Statement**: What is the average transaction amount by transaction type (`deposit` / `withdrawal` / `transfer`)?

---

## Directory Structure
```text
p3/
├── AvgTransactionAmountByType.java  # Standalone single-file MapReduce solution
├── pom.xml                          # Maven build file for packaging JAR
├── README.md                        # Quick reference documentation
├── EXPLANATION.md                   # Comprehensive technical report & architecture
├── src/main/java/trustbank/p3/
│   ├── AvgTransactionMapper.java   # MapReduce Mapper
│   ├── AvgTransactionReducer.java  # MapReduce Reducer
│   └── AvgTransactionDriver.java   # MapReduce Driver with ToolRunner
└── output/
    ├── part-r-00000                 # Real Hadoop MapReduce output
    ├── _SUCCESS                     # Hadoop job success marker
    └── job_execution.log            # Complete execution console log
```

---

## Quick Execution

### 1. Build JAR
```bash
mvn clean package
```
Builds `target/trustbank-p3.jar`.

### 2. Run on Hadoop / YARN
```bash
hadoop jar target/trustbank-p3.jar trustbank.p3.AvgTransactionDriver \
    /path/to/dataset/transactions.csv \
    /path/to/output
```

### 3. Check Results
```bash
cat output/part-r-00000
```

---

## Output
```text
deposit	16621.64
transfer	18377.92
withdrawal	17640.68
```

For full architecture diagrams, data flow breakdown, and business analysis, see [EXPLANATION.md](EXPLANATION.md).
