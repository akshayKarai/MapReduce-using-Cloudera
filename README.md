# MapReduce Using Cloudera

A Hadoop MapReduce project implemented in Java and executed using the **Cloudera Hadoop environment**. This project demonstrates document processing, word counting, term-frequency calculation, TF-IDF computation, and keyword-based document search.

## 📌 Project Overview

This project uses **Apache Hadoop MapReduce** to process a collection of text documents and build a simple information-retrieval pipeline.

The workflow consists of four primary stages:

```text
Input Documents
      │
      ▼
┌─────────────────┐
│  DocWordCount   │
│ Document Counts │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TermFrequency   │
│      (TF)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      TF-IDF     │
│ Term Importance │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Search      │
│ Keyword Search  │
└─────────────────┘
```

## 📚 References

* [Apache Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html)
* [Cloudera WordCount Tutorial](https://www.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_usage.html)

---

## 📁 Project Files

| File                 | Description                                               |
| -------------------- | --------------------------------------------------------- |
| `DocWordCount.java`  | Calculates the number of words in each document           |
| `TermFrequency.java` | Calculates term frequency (TF) for words within documents |
| `TFIDF.java`         | Calculates TF-IDF scores for terms across documents       |
| `Search.java`        | Searches documents using TF-IDF scores                    |
| `cantrbry/`          | Directory containing the input text documents             |

---

## 🛠️ Technologies Used

* **Java**
* **Apache Hadoop**
* **Hadoop MapReduce**
* **HDFS**
* **Cloudera**
* **Linux/Unix command line**

---

# 🚀 Setup

The examples below assume Hadoop is installed in the Cloudera environment and that the Hadoop command-line tools are available.

## 1. Create the Project Directory

Create a project directory in HDFS:

```bash
hadoop fs -mkdir /user/cloudera/project/
```

Create a directory for the input documents:

```bash
hadoop fs -mkdir /user/cloudera/project/input
```

## 2. Upload Input Documents

Upload the documents from the local `cantrbry` directory to HDFS:

```bash
hadoop fs -put cantrbry/* /user/cloudera/project/input
```

Verify that the files were uploaded successfully:

```bash
hadoop fs -ls /user/cloudera/project/input
```

---

# 1️⃣ Document Word Count

`DocWordCount.java` calculates the word count for each document in the input dataset.

## Compile

Create a build directory:

```bash
mkdir -p build
```

Compile the Java source:

```bash
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* \
    DocWordCount.java \
    -d build \
    -Xlint
```

Create the JAR file:

```bash
jar -cvf docWordCount.jar -C build/ .
```

## Run MapReduce Job

Execute the MapReduce job against the input documents:

```bash
hadoop jar docWordCount.jar \
    org.myorg.DocWordCount \
    /user/cloudera/project/input \
    /user/cloudera/project/docwordcount/output
```

> **Note:** The output directory must not already exist. If the job has been executed previously, remove the existing output directory before running it again.

```bash
hadoop fs -rm -r /user/cloudera/project/docwordcount/output
```

## View Results

Display the output directly from HDFS:

```bash
hadoop fs -cat /user/cloudera/project/docwordcount/output/*
```

Copy the output files from HDFS to the current local directory:

```bash
hadoop fs -copyToLocal \
    /user/cloudera/project/docwordcount/output/* \
    .
```

---

# 2️⃣ Term Frequency

`TermFrequency.java` calculates the **Term Frequency (TF)** of words within the documents.

Term Frequency measures how frequently a term occurs within a particular document.

## Compile

```bash
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* \
    TermFrequency.java \
    -d build \
    -Xlint
```

Create the JAR:

```bash
jar -cvf termFrequency.jar -C build/ .
```

## Run MapReduce Job

```bash
hadoop jar termFrequency.jar \
    org.myorg.TermFrequency \
    /user/cloudera/project/input \
    /user/cloudera/project/termfrequency/output
```

## View Results

Display the output:

```bash
hadoop fs -cat \
    /user/cloudera/project/termfrequency/output/*
```

Copy the results locally:

```bash
hadoop fs -copyToLocal \
    /user/cloudera/project/termfrequency/output/* \
    .
```

---

# 3️⃣ TF-IDF

`TFIDF.java` calculates **TF-IDF (Term Frequency-Inverse Document Frequency)** scores.

TF-IDF is commonly used in information retrieval to determine how important a term is to a document within a collection of documents.

Conceptually:

```text
TF-IDF = TF × IDF
```

Where:

* **TF (Term Frequency)** measures how frequently a term appears in a document.
* **IDF (Inverse Document Frequency)** measures how rare or common the term is across the document collection.
* A higher TF-IDF score generally indicates that a term is more representative of a particular document.

## Compile

```bash
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* \
    TFIDF.java \
    -d build \
    -Xlint
```

Create the JAR:

```bash
jar -cvf tfidf.jar -C build/ .
```

## Run MapReduce Job

```bash
hadoop jar tfidf.jar \
    org.myorg.TFIDF \
    /user/cloudera/project/input \
    /user/cloudera/project/tfidf/output
```

## View Results

The final TF-IDF results are stored in the `final` directory.

Display the results:

```bash
hadoop fs -cat \
    /user/cloudera/project/tfidf/output/final/*
```

Copy the results locally:

```bash
hadoop fs -copyToLocal \
    /user/cloudera/project/tfidf/output/final/* \
    .
```

---

# 4️⃣ Document Search

`Search.java` uses the calculated TF-IDF values to search the document collection based on a user-provided query.

## Compile

```bash
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* \
    Search.java \
    -d build \
    -Xlint
```

Create the JAR:

```bash
jar -cvf search.jar -C build/ .
```

## Run a Search

The search program takes the TF-IDF output as input and accepts a search query.

### Query 1: `computer science`

```bash
hadoop jar search.jar \
    org.myorg.Search \
    /user/cloudera/project/tfidf/output/final \
    /user/cloudera/project/search/output \
    computer science
```

### Query 2: `data analysis`

```bash
hadoop jar search.jar \
    org.myorg.Search \
    /user/cloudera/project/tfidf/output/final \
    /user/cloudera/project/search/output \
    data analysis
```

## View Search Results

Display the search results:

```bash
hadoop fs -cat \
    /user/cloudera/project/search/output/*
```

Copy the search results to the local directory:

```bash
hadoop fs -copyToLocal \
    /user/cloudera/project/search/output/* \
    .
```

---

# 🔄 Complete Execution Workflow

The complete project can be executed in the following order:

### Step 1: Upload Documents

```bash
hadoop fs -put cantrbry/* /user/cloudera/project/input
```

### Step 2: Run Document Word Count

```text
DocWordCount.java
       ↓
docwordcount/output
```

### Step 3: Calculate Term Frequency

```text
TermFrequency.java
       ↓
termfrequency/output
```

### Step 4: Calculate TF-IDF

```text
TFIDF.java
       ↓
tfidf/output/final
```

### Step 5: Search Documents

```text
Search.java
       ↓
search/output
```

The resulting pipeline is:

```text
               ┌──────────────────────┐
               │   cantrbry/*.txt     │
               │   Input Documents    │
               └──────────┬───────────┘
                          │
                          ▼
               ┌──────────────────────┐
               │   DocWordCount.java  │
               │  Document Word Count │
               └──────────┬───────────┘
                          │
                          ▼
               ┌──────────────────────┐
               │ TermFrequency.java   │
               │    Term Frequency    │
               └──────────┬───────────┘
                          │
                          ▼
               ┌──────────────────────┐
               │     TFIDF.java       │
               │     TF-IDF Scores    │
               └──────────┬───────────┘
                          │
                          ▼
               ┌──────────────────────┐
               │      Search.java     │
               │  Document Retrieval  │
               └──────────────────────┘
```

---

# 📂 HDFS Directory Structure

After executing the different stages, the HDFS project structure looks approximately like this:

```text
/user/cloudera/project/
│
├── input/
│   ├── document1
│   ├── document2
│   ├── document3
│   └── ...
│
├── docwordcount/
│   └── output/
│
├── termfrequency/
│   └── output/
│
├── tfidf/
│   └── output/
│       └── final/
│
└── search/
    └── output/
```

---

# 🧹 Cleaning Previous Output

Hadoop MapReduce jobs generally require the output directory to **not already exist**.

If you need to rerun a job, remove its previous output directory first.

For example:

```bash
hadoop fs -rm -r /user/cloudera/project/docwordcount/output
```

For the complete project:

```bash
hadoop fs -rm -r /user/cloudera/project/docwordcount/output
hadoop fs -rm -r /user/cloudera/project/termfrequency/output
hadoop fs -rm -r /user/cloudera/project/tfidf/output
hadoop fs -rm -r /user/cloudera/project/search/output
```

---

# 🎯 Learning Objectives

This project demonstrates practical experience with:

* Hadoop Distributed File System (HDFS)
* Java-based MapReduce programming
* Mapper and Reducer workflows
* Distributed text processing
* Word counting
* Term Frequency calculation
* TF-IDF calculation
* Information retrieval
* Keyword-based document search
* Hadoop JAR compilation and execution
* Working with input and output data in HDFS

---

# 📝 Notes

* The commands in this README assume a **Cloudera Hadoop environment**.
* Hadoop installation paths may differ depending on the Cloudera/CDH version.
* The package name used in the commands is `org.myorg`.
* Update the package/class names in the commands if your Java source files use a different package.
* HDFS output directories should be removed before rerunning the corresponding MapReduce job.
* The `cantrbry` directory contains the text corpus used as the input dataset.

---

## 📖 Additional Resources

* [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
* [Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html)
* [Cloudera Documentation](https://docs.cloudera.com/)

---

## 👨‍💻 Project

**MapReduce Using Cloudera**

A Java and Hadoop MapReduce implementation demonstrating document processing and basic information retrieval using TF, TF-IDF, and keyword search.
