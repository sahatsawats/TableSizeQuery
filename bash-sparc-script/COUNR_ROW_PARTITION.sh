#!/bin/bash

# List of partition names you want to query
partitions=("P1506" "P1507" "P1508" "P1509" "P1510" "P1511" "P1512" "P1513" "P1514" "P1515" "P1516" "P1517" "P1518" "P1519" "P1520" "P1521" "P1522" "P1523" "P1524" "P1525" "P1526" "P1527" "P1528" "P1529" "P1530" "P1531" "P1532" "P1533" "P1534" "P1535" "P1536" "P1537" "P1538")


# Output file to store the results
output_file="./partition_counts.txt"

# Create or clear the output file before starting
> $output_file

# Start time monitoring
start_time=$(date +%s)

# Loop through each partition name
for partition in "${partitions[@]}"; do
    # Execute the COUNT query for each partition

    # Execute the query and append the result to the output file
    row_count=$(sqlplus -S test2/oracle4u <<EOF
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
SET LINESIZE 80;
SELECT COUNT(*) FROM GENEVA_ADMIN.COSTEDEVENT PARTITION($partition);
EXIT;
EOF
    )
    
    # Append the result in the format "partition_name,*ROW_COUNT" to the output file
    echo "$partition,$row_count" >> $output_file
    echo "Appended result for partition: $partition"
done

# End time monitoring
end_time=$(date +%s)

# Calculate the elapsed time
elapsed_time=$((end_time - start_time))


echo "Results saved to $output_file with time_consumed: $elapsed_time"
