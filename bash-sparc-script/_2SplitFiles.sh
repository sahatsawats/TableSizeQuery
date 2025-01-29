#!/bin/bash

# Input file containing OWNER.TABLE_NAME
INPUT_FILE="tables_with_owners.txt"

# Check if input file exists and is not empty
if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Error: Input file '$INPUT_FILE' does not exist."
  exit 1
elif [[ ! -s "$INPUT_FILE" ]]; then
  echo "Error: Input file '$INPUT_FILE' is empty."
  exit 1
fi

# Number of smaller files to create
NUM_FILES=4

# Prefix for the smaller files
OUTPUT_PREFIX="tables_part_"

# Count the total number of lines in the input file
TOTAL_LINES=$(wc -l < "$INPUT_FILE")

# Calculate the number of lines per file
LINES_PER_FILE=$(( (TOTAL_LINES + NUM_FILES - 1) / NUM_FILES )) # Round up

# Debugging output
echo "Splitting '$INPUT_FILE' into $NUM_FILES files with approximately $LINES_PER_FILE lines each."

# Split the file into smaller files with numeric suffixes
split -l "$LINES_PER_FILE" -a 2 "$INPUT_FILE" "$OUTPUT_PREFIX"


# Initialize a sum for all split files
SUM_LINES=0

# Output the total number of lines per split file
echo "Original file: $INPUT_FILE has $TOTAL_LINES lines."
echo "Lines per split file:"
for FILE in ${OUTPUT_PREFIX}*; do
  if [[ -f "$FILE" ]]; then
    FILE_LINES=$(wc -l < "$FILE")
    echo "$FILE: $FILE_LINES lines"
    SUM_LINES=$((SUM_LINES + FILE_LINES))
  else
    echo "Warning: File '$FILE' not found."
  fi
done

# Print the sum of lines across all split files
echo "Sum of lines in all split files: $SUM_LINES lines"

# Verify if the sum matches the original file's line count
if [[ "$SUM_LINES" -eq "$TOTAL_LINES" ]]; then
  echo "Validation successful: The sum matches the original file's line count."
else
  echo "Validation failed: The sum does not match the original file's line count."
fi