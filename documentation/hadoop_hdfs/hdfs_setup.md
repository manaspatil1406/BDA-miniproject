# FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System using Big Data
## Stage 3 – Hadoop HDFS Integration and Single-Node Setup Guide

---

## 1. Executive Summary & Objective

**Stage 3** transitions the **FoodieAnalytics** project from local data preprocessing to enterprise-grade **Big Data Distributed Storage** using the **Hadoop Distributed File System (HDFS)**. 

### Core Objectives
1. Deploy and configure **Apache Hadoop 3.4.3** in a **Single-Node Pseudo-Distributed Cluster** on **Ubuntu 24.04 LTS (WSL2)** with **OpenJDK 21**.
2. Establish master and worker daemons (**NameNode**, **DataNode**, **SecondaryNameNode**) on `localhost:9000`.
3. Configure optimal single-node storage policies, including **replication factor 1** (`dfs.replication = 1`).
4. Establish the project namespace hierarchy `/foodieanalytics/data` within HDFS.
5. Ingest the curated dataset (`dataset/processed/cleaned_zomato.csv`, ~547.93 MB, 51,656 records) into HDFS.
6. Verify block integrity, directory permissions, file accessibility, and web monitoring via the **NameNode Web UI** (`http://localhost:9870`).

---

## 2. Hadoop Architecture for Single-Node Environment

In enterprise deployments, Apache Hadoop runs across thousands of commodity nodes. For development, prototyping, and academic demonstration, Hadoop operates in **Pseudo-Distributed Mode**, where all HDFS daemons run as independent Java processes on a single host.

```text
+-----------------------------------------------------------------------------------+
|                            Ubuntu 24.04 LTS (WSL2)                                |
|                                                                                   |
|   +-----------------------+                    +------------------------------+   |
|   |  NameNode (Master)    |                    | SecondaryNameNode (Helper)   |   |
|   |  * Port: 9000 (RPC)   |                    | * Checkpoints FsImage &      |   |
|   |  * Port: 9870 (WebUI) |                    |   EditLog periodically       |   |
|   |  * Manages Namespace  |                    | * Port: 9868                 |   |
|   |  * In-Memory Metadata |                    +------------------------------+   |
|   +-----------+-----------+                                                       |
|               | (Heartbeats, Block Reports)                                       |
|               v                                                                   |
|   +---------------------------------------------------------------------------+   |
|   |  DataNode (Worker / Storage)                                              |   |
|   |  * Port: 9866 (Data transfer), 9864 (Web UI)                              |   |
|   |  * Stores actual HDFS data blocks on local filesystem                     |   |
|   |  * Block size: 128 MB                                                     |   |
|   |  * Replication Factor: 1 (Single Node)                                    |   |
|   +---------------------------------------------------------------------------+   |
+-----------------------------------------------------------------------------------+
                                        |
               HDFS Namespace: /foodieanalytics/data/cleaned_zomato.csv
            (~547.93 MB partitioned into ~5 physical 128 MB storage blocks)
```

### 2.1 Daemon Breakdown
1. **NameNode (Master Daemon)**:
   * Acts as the directory master for HDFS.
   * Maintains the directory tree (`/foodieanalytics/data`) and tracks the physical location of each 128 MB block on the DataNode.
   * Operates an RPC server on `hdfs://localhost:9000` and an HTTP management dashboard on `http://localhost:9870`.
2. **DataNode (Worker Daemon)**:
   * Stores the raw binary blocks of data.
   * Sends periodic heartbeats and block reports back to the NameNode to confirm storage health.
3. **SecondaryNameNode (Checkpointing Daemon)**:
   * Periodically merges the transaction log (`edits`) with the filesystem state (`fsimage`).
   * Prevents the edit log on the NameNode from growing excessively large, ensuring rapid startup times.

### 2.2 Replication Factor 1 Explanation
In a multi-node cluster, Hadoop defaults to a replication factor of 3 (`dfs.replication = 3`), copying each block across three separate physical servers for rack-aware fault tolerance. 

In a **Single-Node Pseudo-Distributed setup**:
* Only **one DataNode** is running.
* Setting `dfs.replication = 1` instructs the NameNode to store exactly one copy of each block.
* If left at the default of 3 on a single-node cluster, the NameNode would permanently report **Under-Replicated Blocks**, producing persistent health warnings because two additional physical DataNodes do not exist.
* Setting `dfs.replication = 1` guarantees zero storage waste and an immediate **HEALTHY** cluster state.

### 2.3 Block Size Mechanics
* Default block size in Hadoop 3.x is **128 MB**.
* Our cleaned dataset `cleaned_zomato.csv` is **~547.93 MB** (574,544,388 bytes).
* In HDFS, this file is automatically split into **5 physical blocks**:
  * Block 1: 128 MB
  * Block 2: 128 MB
  * Block 3: 128 MB
  * Block 4: 128 MB
  * Block 5: ~35.93 MB
* Each block is assigned a globally unique 64-bit Block ID (`blk_...`) and stored in the DataNode directory.

---

## 3. Environment Specifications

| Parameter | Selected Value | Description |
| :--- | :--- | :--- |
| **Operating System** | Ubuntu 24.04.5 LTS | Running inside Windows Subsystem for Linux (WSL2) |
| **Java Development Kit** | OpenJDK 21 (64-Bit) | Runtime Environment (build 21.0.12.1+1-1-24.04.4-Ubuntu) |
| **Java Home Path** | `/usr/lib/jvm/java-21-openjdk-amd64` | Configured in `~/.bashrc` and `hadoop-env.sh` |
| **Hadoop Distribution** | Apache Hadoop 3.4.3 | Binary release for Linux x86_64 |
| **Hadoop Install Path** | `/home/manas_patil/apps/hadoop-3.4.3` | Configured as `HADOOP_HOME` |
| **HDFS NameNode URI** | `hdfs://localhost:9000` | Default filesystem target (`fs.defaultFS`) |
| **NameNode Web UI** | `http://localhost:9870` | Web administration and directory browser interface |
| **Replication Factor** | `1` | Optimized for single-node development cluster |
| **Target HDFS Directory** | `/foodieanalytics/data` | Project storage namespace |
| **Ingested Dataset** | `cleaned_zomato.csv` | Output from Stage 2 data cleaning |

---

## 4. Step-by-Step Installation & Configuration Workflow

### Step 4.1: Verify Java & Passwordless SSH
Hadoop start scripts (`start-dfs.sh`) connect to cluster nodes via SSH, even on localhost.

```bash
# 1. Verify Java OpenJDK 21
java -version

# 2. Install OpenSSH server if not installed
sudo apt update && sudo apt install -y openssh-server openssh-client

# 3. Generate SSH key pair without passphrase
ssh-keygen -t ed25519 -P "" -f ~/.ssh/id_ed25519

# 4. Authorize key for passwordless localhost login
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# 5. Verify passwordless connection
ssh localhost
exit
```

---

### Step 4.2: Configure Environment Variables (`~/.bashrc`)
Add Hadoop and Java paths to `~/.bashrc` for automated terminal environment loading:

```bash
# Java Environment
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Hadoop Environment
export HADOOP_HOME=/home/manas_patil/apps/hadoop-3.4.3
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
```

Apply changes immediately:
```bash
source ~/.bashrc
```

---

### Step 4.3: Configure Hadoop XML Configuration Files
Navigate to `$HADOOP_HOME/etc/hadoop`:

#### A. `hadoop-env.sh`
Explicitly set `JAVA_HOME` inside Hadoop's environment script:
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

#### B. `core-site.xml`
Defines the default filesystem protocol and NameNode host/port:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
        <description>Default file system URI for NameNode RPC</description>
    </property>
</configuration>
```

#### C. `hdfs-site.xml`
Defines replication factor and local directories for metadata and block storage:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <description>Block replication factor for single-node cluster</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/manas_patil/apps/hadoop-3.4.3/data/dfs/namenode</value>
        <description>Local filesystem directory for NameNode metadata storage</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/manas_patil/apps/hadoop-3.4.3/data/dfs/datanode</value>
        <description>Local filesystem directory for DataNode block storage</description>
    </property>
</configuration>
```

---

### Step 4.4: Format the NameNode (Initial Setup Only)

> [!IMPORTANT]
> **SAFETY RULE**: The NameNode format operation (`hdfs namenode -format`) must be executed **strictly once** during initial cluster setup. Formatting creates the initial `fsimage` file and assigns a new Cluster ID. Executing this command on an established cluster deletes the namespace and causes Cluster ID mismatches with existing DataNodes!

```bash
hdfs namenode -format
```

Successful execution outputs:
```text
INFO common.Storage: Storage directory ... has been successfully formatted.
INFO namenode.FSImage: Allocated new BlockPoolId: BP-...
```

---

### Step 4.5: Start Hadoop HDFS Daemons

Start the distributed filesystem services:
```bash
start-dfs.sh
```

Verify active daemons using the Java Virtual Machine Process Status Tool (`jps`):
```bash
jps
```

Expected output confirms all three core daemons are running:
```text
28412 NameNode
28605 DataNode
28841 SecondaryNameNode
29104 Jps
```

---

### Step 4.6: Create HDFS Directory Hierarchy

Create the dedicated project directory `/foodieanalytics/data` in HDFS:
```bash
hdfs dfs -mkdir -p /foodieanalytics/data
```

Verify directory creation:
```bash
hdfs dfs -ls /foodieanalytics
```

---

### Step 4.7: Upload Cleaned Dataset into HDFS

Using the script or manual command, ingest the curated CSV:
```bash
# Upload command
hdfs dfs -put dataset/processed/cleaned_zomato.csv /foodieanalytics/data/
```

Verify file presence and permissions:
```bash
hdfs dfs -ls -h /foodieanalytics/data
```

Expected output:
```text
Found 1 items
-rw-r--r--   1 manas_patil supergroup    547.9 M 2026-11-03 20:30 /foodieanalytics/data/cleaned_zomato.csv
```

---

### Step 4.8: Verify HDFS Content & Readability

```bash
# Display disk usage
hdfs dfs -du -h /foodieanalytics/data

# Inspect the header and first 5 records directly from HDFS
hdfs dfs -cat /foodieanalytics/data/cleaned_zomato.csv | head -n 5

# Confirm record count (Header + 51,656 data records = 51,657 lines)
hdfs dfs -cat /foodieanalytics/data/cleaned_zomato.csv | wc -l
```

---

## 5. Automation Scripts

Two production-grade Bash scripts are provided in `scripts/hdfs_integration/`:

| Script | Location | Purpose | Execution Command |
| :--- | :--- | :--- | :--- |
| **Upload Script** | [`scripts/hdfs_integration/upload_to_hdfs.sh`](file:///c:/BDA-miniproject/scripts/hdfs_integration/upload_to_hdfs.sh) | Validates local dataset existence, checks NameNode connectivity, ensures `/foodieanalytics/data` exists, and uploads the dataset. | `bash scripts/hdfs_integration/upload_to_hdfs.sh` |
| **Verification Script** | [`scripts/hdfs_integration/verify_hdfs.sh`](file:///c:/BDA-miniproject/scripts/hdfs_integration/verify_hdfs.sh) | Audits `jps` daemons, tests HDFS connectivity, lists directory contents, checks disk usage, prints sample records, and verifies line count. | `bash scripts/hdfs_integration/verify_hdfs.sh` |

---

## 6. Demonstration & Evaluation Evidence Checklist

During academic evaluation, demonstrate the following six checkpoints:

| # | Demonstration Item | Terminal Command / URL | Evaluation Verification |
| :---: | :--- | :--- | :--- |
| **1** | **Hadoop & Java Versions** | `hadoop version`<br>`java -version` | Proves Hadoop 3.4.3 and OpenJDK 21 are running. |
| **2** | **Active HDFS Daemons** | `jps` | Displays running `NameNode`, `DataNode`, and `SecondaryNameNode` processes. |
| **3** | **HDFS Directory Listing** | `hdfs dfs -ls -h /foodieanalytics/data` | Proves `cleaned_zomato.csv` exists with permission `-rw-r--r--`, replication `1`, and size `~547.9 M`. |
| **4** | **HDFS Disk Usage** | `hdfs dfs -du -h /foodieanalytics/data` | Shows total storage consumed by the dataset on the DataNode. |
| **5** | **Direct HDFS Data Preview** | `hdfs dfs -cat /foodieanalytics/data/cleaned_zomato.csv \| head -n 5` | Reads the standardized schema and data records directly from distributed storage. |
| **6** | **NameNode Web UI** | Browser: `http://localhost:9870` | Navigates to **Utilities** $\rightarrow$ **Browse the file system** to show `/foodieanalytics/data/cleaned_zomato.csv`. |

---

## 7. Troubleshooting & Common Pitfalls

| Issue | Root Cause | Resolution Strategy |
| :--- | :--- | :--- |
| **`Connection refused` on `localhost:9000`** | NameNode daemon is stopped or failed to bind to port 9000. | Check `$HADOOP_HOME/logs/hadoop-*-namenode-*.log`. Ensure `start-dfs.sh` has been executed. |
| **DataNode missing in `jps`** | Reformatting NameNode generated a new Cluster ID while DataNode still has old ID. | Never reformat NameNode. If required, clear both `namenode/` and `datanode/` directories before re-running `hdfs namenode -format`. |
| **`Permission denied (publickey)`** | SSH passwordless authentication is not configured for `localhost`. | Re-run `cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys` and verify with `ssh localhost`. |
| **`Name node is in safe mode`** | NameNode enters safe mode on startup to inspect block reports. | Wait ~30 seconds for safe mode to disengage, or force leave with: `hdfs dfsadmin -safemode leave`. |
| **Under-replicated block warnings** | Default replication factor is 3, but only 1 DataNode exists. | Ensure `<name>dfs.replication</name><value>1</value>` is specified in `hdfs-site.xml`. |

---

## 8. Conclusion and Transition to Stage 4

Stage 3 has successfully established the distributed storage foundation for **FoodieAnalytics**. The cleaned dataset (`cleaned_zomato.csv`) is safely persisted, partitioned across blocks, and accessible via the HDFS RPC protocol on port 9000.

This sets the stage for downstream Big Data compute operations:
* **Apache Hive**: Defining external tables directly pointing to `hdfs://localhost:9000/foodieanalytics/data/`.
* **Apache Spark**: Instantiating Spark DataFrames directly reading from `hdfs://localhost:9000/foodieanalytics/data/cleaned_zomato.csv` for distributed machine learning and restaurant recommendation modeling.

