#!/bin/bash

# Input file containing OWNER.TABLE_NAME
INPUT_FILE="tables_with_owners.txt"

# Number of smaller files to create
NUM_FILES=4

# Prefix for the smaller files
OUTPUT_PREFIX="tables_part_"

# Count the total number of lines in the input file
TOTAL_LINES=$(wc -l < "$INPUT_FILE")

# Calculate the number of lines per file
LINES_PER_FILE=$(( (TOTAL_LINES + NUM_FILES - 1) / NUM_FILES )) # Round up

# Split the file into smaller files
split -l "$LINES_PER_FILE" "$INPUT_FILE" "$OUTPUT_PREFIX"

# Rename the smaller files with appropriate numbering
for i in $(seq 1 $NUM_FILES); do
  mv "${OUTPUT_PREFIX}$(printf '%02d' $((i - 1)))" "${OUTPUT_PREFIX}${i}.txt"
done

echo "Split completed. Files created:"
ls ${OUTPUT_PREFIX}*.txt