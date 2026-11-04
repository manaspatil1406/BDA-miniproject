
#!/bin/bash

set -e

STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"

INPUT="/foodieanalytics/data/hadoop_safe_zomato.csv"
OUTPUT="/foodieanalytics/output/job2_average_rating"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting Job 2: Average Rating by Locality"

if [ ! -f "$STREAMING_JAR" ]; then
    echo "ERROR: Hadoop Streaming JAR not found."
    exit 1
fi

hdfs dfs -test -e "$INPUT" || {
    echo "ERROR: HDFS input dataset not found."
    exit 1
}

if hdfs dfs -test -e "$OUTPUT"; then
    echo "ERROR: Output directory already exists: $OUTPUT"
    echo "Do not delete it automatically. Use a new output path."
    exit 1
fi

hadoop jar "$STREAMING_JAR" \
    -input "$INPUT" \
    -output "$OUTPUT" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file "$SCRIPT_DIR/mapper.py" \
    -file "$SCRIPT_DIR/reducer.py"

echo "Job 2 completed successfully."
echo "Output: $OUTPUT"
