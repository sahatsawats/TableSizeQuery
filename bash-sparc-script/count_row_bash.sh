#!/bin/bash

# Set Oracle environment variables
#export ORACLE_HOME=/path/to/oracle/home
#export PATH=$ORACLE_HOME/bin:$PATH
#export ORACLE_SID=your_oracle_sid

# Database connection parameters

# Database connection parameters
USER="uthai888"
PASS="oracle4u"
SRV="PNEWNMDB_PREFER1"


# Excluded owners (comma-separated, with quotes around each owner name)
EXCLUDED_OWNERS="'REMOTE_SCHEDULER_AGENT','OJVMSYS','GSMADMIN_INTERNAL','DVSYS','DVF','DBSFWUSER','DBAOPER','AUDSYS','ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS','MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','OWBSYS_AUDIT','ORDPLUGINS','ORDSYS','ORDDATA','OUTLN','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST','WKSYS','WKPROXY','WMSYS','XDB','APEX_PUBLIC_USER','APEX_030200','APPQOSSYS','DIP','FLOWS_30000','FLOWS_030000','FLOWS_FILES','MDDATA','ORACLE_OCM','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','XS$NULL','BI','HR','OE','PM','IX','SH','SCOTT','ASMSNMP','PERFSTAT','DBVISIT'"

# Output file
OUTPUT_FILE="tables_with_owners.txt"
# Capture start time
START_TIME=$(date +%s)

# SQL query to fetch table names and owners, excluding specified owners
SQL_QUERY="SELECT owner || '.' || table_name FROM all_tables WHERE owner NOT IN ($EXCLUDED_OWNERS) ORDER BY owner, table_name;"

# Query the database and write output to a file
echo "Querying tables excluding owners: $EXCLUDED_OWNERS"
echo "Results will be saved to $OUTPUT_FILE"

sqlplus -s $USER/$PASS@$SRV <<EOF
SET PAGESIZE 0
SET LINESIZE 200
SET HEADING OFF
SET FEEDBACK OFF
SET NEWPAGE NONE
SPOOL $OUTPUT_FILE
$SQL_QUERY
SPOOL OFF
EOF

echo "Query completed in $ELAPSED_TIME seconds. Results saved in $OUTPUT_FILE."
