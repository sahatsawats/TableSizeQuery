#!/bin/bash


# Database connection parameters
USER=""
PASS=""
SRV=""
INPUT_FILE=""
# Parse options
while getopts "u:s:f:p:" opt; do
  case "$opt" in
    u) USER="$OPTARG" ;;
    s) SRV="$OPTARG" ;;
    p) PASS="$OPTARG" ;;
    f) INPUT_FILE="$OPTARG";;
    *) echo "Usage: $0 -u <user> -s <server> -p <password>" >&2; exit 1 ;;
  esac
done

if [[ -z "$USER" || -z "$SRV" || -z "$PASS" || -z "$INPUT_FILE" ]]; then
  echo "Error: All options -u, -s, -f,and -p are required." >&2
  echo "Usage: $0 -u <user> -s <service> -p <password> -f <file>" >&2
  exit 1
fi


# Output CSV file
OUTPUT_CSV="table_row_counts.csv"

# Capture start time
START_TIME=$(perl -e 'print time')

# Read each line from the input file and query row count
while IFS= read -r table; do
  # Extract the owner and table name from the line
  OWNER=$(echo $table | cut -d '.' -f 1)
  TABLE_NAME=$(echo $table | cut -d '.' -f 2)

  # SQL query to count rows in the table
  SQL_QUERY="SELECT /*+ parallel(10) */ COUNT(*) FROM $OWNER.$TABLE_NAME;"

  # Run the SQL query and capture the result
  ROW_COUNT=$(sqlplus -s $USER/$PASS@$SRV <<EOF
SET PAGESIZE 0
SET LINESIZE 200
SET HEADING OFF
SET FEEDBACK OFF
$SQL_QUERY
EOF
)

  # Remove any leading or trailing whitespace from ROW_COUNT
  ROW_COUNT=$(echo "$ROW_COUNT" | xargs)

  # Append the result to the CSV file
  echo "$OWNER,$TABLE_NAME,$ROW_COUNT" >> "$OUTPUT_CSV"
  
done < "$INPUT_FILE"

# Get the end time (in seconds) using Perl
END_TIME=$(perl -e 'print time')

# Calculate the elapsed time
ELAPSED_TIME=$((END_TIME - START_TIME))


# Output message
echo "Row counts saved to $OUTPUT_CSV with $ELAPSED_TIME seconds"
