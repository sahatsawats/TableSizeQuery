#!/bin/bash


# Database connection parameters
USER="uthai888"
PASS="oracle4u"
SRV="PNEWNMDB_PREFER1"

# Input file containing OWNER.TABLE_NAME
INPUT_FILE="tables_with_owners.txt"

# Output CSV file
OUTPUT_CSV="table_row_counts.csv"

# Write the CSV header
echo "Owner,Table Name,Row Count" > "$OUTPUT_CSV"

# Read each line from the input file and query row count
while IFS= read -r table; do
  # Extract the owner and table name from the line
  OWNER=$(echo $table | cut -d '.' -f 1)
  TABLE_NAME=$(echo $table | cut -d '.' -f 2)

  # SQL query to count rows in the table
  SQL_QUERY="SELECT COUNT(*) FROM $OWNER.$TABLE_NAME;"

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

# Output message
echo "Row counts saved to $OUTPUT_CSV."
