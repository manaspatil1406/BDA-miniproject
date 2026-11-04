#!/usr/bin/env bash
# ==============================================================================
# FoodieAnalytics: Food Delivery Analytics & Restaurant Recommendation System
# Stage 3: Hadoop HDFS Integration - Dataset Upload Script
# ==============================================================================
# This script uploads the cleaned Zomato restaurant dataset from the local
# repository into the Hadoop Distributed File System (HDFS).
# Compatible with Ubuntu 24.04 LTS on WSL2 (Hadoop 3.4.3 / Java OpenJDK 21).
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 1. Environment & Path Resolution
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Target paths
LOCAL_DATASET="${PROJECT_ROOT}/dataset/processed/cleaned_zomato.csv"
HDFS_DIR="/foodieanalytics/data"
HDFS_TARGET_FILE="${HDFS_DIR}/cleaned_zomato.csv"

# Color formatting for terminal output
BOLD="\033[1m"
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${BLUE}   FOODIEANALYTICS: HDFS DATASET INGESTION PIPELINE (STAGE 3)                 ${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "Timestamp       : $(date '+%Y-%m-%d %H:%M:%S')"
echo -e "Project Root    : ${PROJECT_ROOT}"
echo -e "Local Dataset   : ${LOCAL_DATASET}"
echo -e "HDFS Directory  : ${HDFS_DIR}"
echo -e "HDFS Destination: ${HDFS_TARGET_FILE}"
echo -e "------------------------------------------------------------------------------"

# ------------------------------------------------------------------------------
# 2. Pre-flight Checks
# ------------------------------------------------------------------------------
# Check 2.1: Verify local cleaned dataset exists
if [[ ! -f "${LOCAL_DATASET}" ]]; then
    echo -e "${RED}[ERROR] Local cleaned dataset not found at:${NC}"
    echo -e "        ${LOCAL_DATASET}"
    echo -e "        Please ensure Stage 2 (data cleaning) has been executed first."
    exit 1
fi
LOCAL_SIZE_BYTES=$(stat -c%s "${LOCAL_DATASET}" 2>/dev/null || stat -f%z "${LOCAL_DATASET}" 2>/dev/null || echo "Unknown")
LOCAL_SIZE_MB=$(awk "BEGIN {printf \"%.2f\", ${LOCAL_SIZE_BYTES}/1048576}" 2>/dev/null || echo "N/A")
echo -e "${GREEN}[OK]${NC} Local cleaned dataset verified (${LOCAL_SIZE_MB} MB / ${LOCAL_SIZE_BYTES} bytes)."

# Check 2.2: Verify Hadoop / HDFS CLI availability
if ! command -v hdfs &> /dev/null; then
    if [[ -n "${HADOOP_HOME:-}" && -x "${HADOOP_HOME}/bin/hdfs" ]]; then
        export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"
    elif [[ -x "/home/manas_patil/apps/hadoop-3.4.3/bin/hdfs" ]]; then
        export HADOOP_HOME="/home/manas_patil/apps/hadoop-3.4.3"
        export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"
    else
        echo -e "${RED}[ERROR] 'hdfs' command not found in PATH and HADOOP_HOME is not set.${NC}"
        echo -e "        Please export HADOOP_HOME and add \$HADOOP_HOME/bin to your PATH."
        exit 1
    fi
fi
echo -e "${GREEN}[OK]${NC} Hadoop HDFS CLI binary located at: $(command -v hdfs)"

# Check 2.3: Verify HDFS NameNode responsiveness
echo -e "\n${BOLD}[Step 1/4] Checking HDFS NameNode Connectivity...${NC}"
if ! hdfs dfs -ls / &> /dev/null; then
    echo -e "${RED}[ERROR] Failed to connect to HDFS NameNode.${NC}"
    echo -e "        Please verify that Hadoop services are running (start-dfs.sh) and"
    echo -e "        NameNode is listening at hdfs://localhost:9000."
    exit 1
fi
echo -e "${GREEN}[OK]${NC} NameNode is active and responding."

# ------------------------------------------------------------------------------
# 3. Create HDFS Project Directory
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 2/4] Ensuring Target HDFS Directory Exists (${HDFS_DIR})...${NC}"
if hdfs dfs -test -d "${HDFS_DIR}" &> /dev/null; then
    echo -e "${GREEN}[INFO]${NC} Directory '${HDFS_DIR}' already exists in HDFS."
else
    echo -e "${YELLOW}[ACTION]${NC} Creating HDFS directory '${HDFS_DIR}' with -p..."
    hdfs dfs -mkdir -p "${HDFS_DIR}"
    echo -e "${GREEN}[OK]${NC} Directory '${HDFS_DIR}' successfully created."
fi

# ------------------------------------------------------------------------------
# 4. Ingest Cleaned Dataset into HDFS
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 3/4] Ingesting Cleaned Dataset into HDFS...${NC}"
if hdfs dfs -test -e "${HDFS_TARGET_FILE}" &> /dev/null; then
    echo -e "${YELLOW}[WARNING]${NC} Target file '${HDFS_TARGET_FILE}' already exists in HDFS."
    echo -e "          Updating / Overwriting existing dataset (-put -f)..."
    hdfs dfs -put -f "${LOCAL_DATASET}" "${HDFS_DIR}/"
else
    echo -e "${BLUE}[ACTION]${NC} Uploading '${LOCAL_DATASET}' to '${HDFS_DIR}/'..."
    hdfs dfs -put "${LOCAL_DATASET}" "${HDFS_DIR}/"
fi
echo -e "${GREEN}[OK]${NC} Dataset uploaded successfully into HDFS."

# ------------------------------------------------------------------------------
# 5. Immediate Verification
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 4/4] Verifying Ingested File in HDFS...${NC}"
echo -e "HDFS File Listing:"
hdfs dfs -ls "${HDFS_DIR}"

echo -e "\nHDFS File Disk Usage:"
hdfs dfs -du -h "${HDFS_DIR}"

echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN} SUCCESS: Stage 3 HDFS Dataset Upload Completed Successfully!                 ${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"
echo -e "HDFS URI       : hdfs://localhost:9000${HDFS_TARGET_FILE}"
echo -e "Web UI Monitor : http://localhost:9870 (Browse Directory: ${HDFS_DIR})"
echo -e "Next Step      : Run 'bash scripts/hdfs_integration/verify_hdfs.sh' for full audit."
echo -e "=============================================================================="

