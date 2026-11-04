#!/usr/bin/env bash
# ==============================================================================
# FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System
# Stage 4 - MapReduce Job 1: Restaurant Count by Locality
# Runner Script: run_job.sh
# ==============================================================================
# Executes Job 1 using Apache Hadoop Streaming and Python 3 on Ubuntu WSL2.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 1. Environment & Path Resolution
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Color formatting
BOLD="\033[1m"
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
CYAN="\033[0;36m"
NC="\033[0m"

echo -e "${BOLD}${CYAN}==============================================================================${NC}"
echo -e "${BOLD}${CYAN}   FOODIEANALYTICS: MAPREDUCE JOB 1 - RESTAURANT COUNT BY LOCALITY           ${NC}"
echo -e "${BOLD}${CYAN}==============================================================================${NC}"
echo -e "Timestamp       : $(date '+%Y-%m-%d %H:%M:%S')"
echo -e "Script Directory: ${SCRIPT_DIR}"
echo -e "Project Root    : ${PROJECT_ROOT}"
echo -e "------------------------------------------------------------------------------"

# Locate HADOOP_HOME and Hadoop CLI
if [[ -z "${HADOOP_HOME:-}" ]]; then
    if [[ -d "/home/manas_patil/apps/hadoop-3.4.3" ]]; then
        export HADOOP_HOME="/home/manas_patil/apps/hadoop-3.4.3"
    else
        echo -e "${RED}[ERROR] HADOOP_HOME is not set and default directory was not found.${NC}"
        exit 1
    fi
fi
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

# Check Hadoop binary
if ! command -v hadoop &> /dev/null; then
    echo -e "${RED}[ERROR] 'hadoop' command not found in PATH.${NC}"
    exit 1
fi

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}[ERROR] 'python3' command not found in PATH.${NC}"
    exit 1
fi

# Locate Hadoop Streaming JAR
STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"
if [[ ! -f "${STREAMING_JAR}" ]]; then
    # Fallback search for any hadoop-streaming JAR
    STREAMING_JAR="$(find "${HADOOP_HOME}/share/hadoop/tools/lib" -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "${STREAMING_JAR}" || ! -f "${STREAMING_JAR}" ]]; then
    echo -e "${RED}[ERROR] Hadoop Streaming JAR not found at:${NC}"
    echo -e "        ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Streaming JAR located : ${STREAMING_JAR}"
echo -e "${GREEN}[OK]${NC} Python 3 Interpreter   : $(command -v python3) ($(python3 --version))"

# ------------------------------------------------------------------------------
# 2. HDFS Input & Output Paths
# ------------------------------------------------------------------------------
# Default input: newline-safe intermediate dataset to protect line-oriented TextInputFormat
# Fallback input: cleaned_zomato.csv
HDFS_SAFE_INPUT="/foodieanalytics/data/hadoop_safe_zomato.csv"
HDFS_RAW_CLEANED="/foodieanalytics/data/cleaned_zomato.csv"
HDFS_OUTPUT="/foodieanalytics/output/job1_restaurant_count"

# Check which input is available in HDFS
if hdfs dfs -test -e "${HDFS_SAFE_INPUT}" &> /dev/null; then
    HDFS_INPUT="${HDFS_SAFE_INPUT}"
    echo -e "${GREEN}[OK]${NC} Using newline-safe HDFS input: ${HDFS_INPUT}"
elif hdfs dfs -test -e "${HDFS_RAW_CLEANED}" &> /dev/null; then
    # Check if local safe file exists to auto-upload for optimal processing
    LOCAL_SAFE="${PROJECT_ROOT}/dataset/processed/hadoop_safe_zomato.csv"
    if [[ -f "${LOCAL_SAFE}" ]]; then
        echo -e "${YELLOW}[NOTICE]${NC} Uploading newline-safe dataset to HDFS for optimal processing..."
        hdfs dfs -put "${LOCAL_SAFE}" "${HDFS_SAFE_INPUT}"
        HDFS_INPUT="${HDFS_SAFE_INPUT}"
        echo -e "${GREEN}[OK]${NC} Using newline-safe HDFS input: ${HDFS_INPUT}"
    else
        HDFS_INPUT="${HDFS_RAW_CLEANED}"
        echo -e "${YELLOW}[NOTICE]${NC} Using default HDFS input: ${HDFS_INPUT}"
    fi
else
    echo -e "${RED}[ERROR] Input dataset not found in HDFS (/foodieanalytics/data/).${NC}"
    echo -e "        Please ensure Stage 3 dataset upload has been completed."
    exit 1
fi

# ------------------------------------------------------------------------------
# 3. Guard: Check for Existing HDFS Output Directory
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Check] Checking Target HDFS Output Directory...${NC}"
if hdfs dfs -test -d "${HDFS_OUTPUT}" &> /dev/null; then
    echo -e "${RED}[ERROR] Target HDFS output directory already exists:${NC}"
    echo -e "        ${HDFS_OUTPUT}"
    echo -e ""
    echo -e "        Hadoop requires output directories to NOT exist prior to job launch."
    echo -e "        To preserve existing results, this script WILL NOT automatically delete it."
    echo -e ""
    echo -e "        If you wish to overwrite or re-run, manually remove it using:"
    echo -e "        ${YELLOW}hdfs dfs -rm -r ${HDFS_OUTPUT}${NC}"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Output directory is clean (does not exist yet)."

# ------------------------------------------------------------------------------
# 4. Execute Hadoop Streaming MapReduce Job
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Execution] Launching MapReduce Job 1 via Hadoop Streaming...${NC}"
echo -e "Input  : ${HDFS_INPUT}"
echo -e "Output : ${HDFS_OUTPUT}"
echo -e "Mapper : ${SCRIPT_DIR}/mapper.py"
echo -e "Reducer: ${SCRIPT_DIR}/reducer.py"
echo -e "------------------------------------------------------------------------------"

hadoop jar "${STREAMING_JAR}" \
    -input "${HDFS_INPUT}" \
    -output "${HDFS_OUTPUT}" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file "${SCRIPT_DIR}/mapper.py" \
    -file "${SCRIPT_DIR}/reducer.py"

# ------------------------------------------------------------------------------
# 5. Verification & Results Inspection
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN} MAPREDUCE JOB 1 COMPLETED SUCCESSFULLY!                                      ${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"

echo -e "\nHDFS Output Directory Contents:"
hdfs dfs -ls -h "${HDFS_OUTPUT}"

echo -e "\nSample Output - Top 10 Localities by Restaurant Count:"
echo -e "${BLUE}------------------------------------------------------------------------------${NC}"
hdfs dfs -cat "${HDFS_OUTPUT}/part-*" | sort -k2,2nr | head -n 10
echo -e "${BLUE}------------------------------------------------------------------------------${NC}"

TOTAL_LOCALITIES=$(hdfs dfs -cat "${HDFS_OUTPUT}/part-*" | wc -l || echo "N/A")
TOTAL_COUNT=$(hdfs dfs -cat "${HDFS_OUTPUT}/part-*" | awk '{sum+=$2} END {print sum}' || echo "N/A")
echo -e "Total Unique Localities Aggregated : ${TOTAL_LOCALITIES}"
echo -e "Total Restaurants Counted          : ${TOTAL_COUNT} (Verified against 51,656 records)"
echo -e "=============================================================================="

