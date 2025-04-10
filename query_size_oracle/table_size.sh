#!/bin/bash

# Database connection parameters
USER=""
PASS=""
SRV=""
OUTPUT_FILE1="./output_table.csv"
OUTPUT_FILE2="./output_index.csv"
OUTPUT_FILE3="./output_part.csv"
# Parse options
while getopts "u:s:p:" opt; do
  case "$opt" in
    u) USER="$OPTARG" ;;
    s) SRV="$OPTARG" ;;
    p) PASS="$OPTARG" ;;
    *) echo "Usage: $0 -u <user> -s <service> -p <password>" >&2; exit 1 ;;
  esac
done

if [[ -z "$USER" || -z "$SRV" || -z "$PASS" ]]; then
  echo "Error: All options -u, -s, and -p are required." >&2
  echo "Usage: $0 -u <user> -s <service> -p <password>" >&2
  exit 1
fi

# Connect to Oracle Database using SQL*Plus and execute query
SQL_QUERY1="SELECT OWNER, SEGMENT_NAME AS TABLE_NAME, ROUND(SUM(BYTES) / (1024*1024), 2) AS SIZE_MB FROM DBA_SEGMENTS WHERE SEGMENT_TYPE = 'TABLE'
and owner not in  ('REMOTE_SCHEDULER_AGENT','OJVMSYS','GSMADMIN_INTERNAL','DVSYS','DVF','DBSFWUSER','DBAOPER','AUDSYS','ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS','MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','OWBSYS_AUDIT','ORDPLUGINS','ORDSYS','ORDDATA','OUTLN','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST','WKSYS','WKPROXY','WMSYS','XDB','APEX_PUBLIC_USER','APEX_030200','APPQOSSYS','DIP','FLOWS_30000','FLOWS_030000','FLOWS_FILES','MDDATA','ORACLE_OCM','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','XS$NULL','BI','HR','OE','PM','IX','SH','SCOTT','ASMSNMP','PERFSTAT','DBVISIT')
GROUP BY OWNER, SEGMENT_NAME;"

  # Run the SQL query and capture the result
sqlplus -s $USER/$PASS@$SRV <<EOF > $OUTPUT_FILE1
-- set colsep ","     -- separate columns with a comma
set pagesize 0   -- No header rows
set trimspool on -- remove trailing blanks
set headsep off  -- this may or may not be useful...depends on your headings.
set linesize X   -- X should be the sum of the column widths
set numw X       -- X should be the length you want for numbers (avoid scientific notation on IDs)
SET FEEDBACK OFF;

SPOOL $OUTPUT_FILE1
$SQL_QUERY1
EOF


SQL_QUERY2="SELECT idx.table_name, idx.index_name, SUM(bytes)/1024/1024 MB
FROM dba_segments seg,
dba_indexes idx WHERE
idx.owner = seg.owner
AND idx.index_name = seg.segment_name
AND seg.owner not in  ('REMOTE_SCHEDULER_AGENT','OJVMSYS','GSMADMIN_INTERNAL','DVSYS','DVF','DBSFWUSER','DBAOPER','AUDSYS','ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS','MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','OWBSYS_AUDIT','ORDPLUGINS','ORDSYS','ORDDATA','OUTLN','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST','WKSYS','WKPROXY','WMSYS','XDB','APEX_PUBLIC_USER','APEX_030200','APPQOSSYS','DIP','FLOWS_30000','FLOWS_030000','FLOWS_FILES','MDDATA','ORACLE_OCM','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','XS$NULL','BI','HR','OE','PM','IX','SH','SCOTT','ASMSNMP','PERFSTAT','DBVISIT')
GROUP BY idx.index_name, idx.table_name;"

sqlplus -s $USER/$PASS@$SRV <<EOF > $OUTPUT_FILE2
-- set colsep ","     -- separate columns with a comma
set pagesize 0   -- No header rows
set trimspool on -- remove trailing blanks
set headsep off  -- this may or may not be useful...depends on your headings.
set linesize X   -- X should be the sum of the column widths
set numw X       -- X should be the length you want for numbers (avoid scientific notation on IDs)
SET FEEDBACK OFF;

SPOOL $OUTPUT_FILE2
$SQL_QUERY2
EOF

SQL_QUERY3="SELECT SEGMENT_NAME, partition_name, bytes/1024/1024 "MB"
FROM dba_segments
WHERE segment_type = 'TABLE PARTITION'
and owner not in  ('REMOTE_SCHEDULER_AGENT','OJVMSYS','GSMADMIN_INTERNAL','DVSYS','DVF','DBSFWUSER','DBAOPER','AUDSYS','ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS','MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','OWBSYS_AUDIT','ORDPLUGINS','ORDSYS','ORDDATA','OUTLN','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST','WKSYS','WKPROXY','WMSYS','XDB','APEX_PUBLIC_USER','APEX_030200','APPQOSSYS','DIP','FLOWS_30000','FLOWS_030000','FLOWS_FILES','MDDATA','ORACLE_OCM','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','XS$NULL','BI','HR','OE','PM','IX','SH','SCOTT','ASMSNMP','PERFSTAT','DBVISIT');"

sqlplus -s $USER/$PASS@$SRV <<EOF > $OUTPUT_FILE3
-- set colsep ","     -- separate columns with a comma
set pagesize 0   -- No header rows
set trimspool on -- remove trailing blanks
set headsep off  -- this may or may not be useful...depends on your headings.
set linesize X   -- X should be the sum of the column widths
set numw X       -- X should be the length you want for numbers (avoid scientific notation on IDs)
SET FEEDBACK OFF;

SPOOL $OUTPUT_FILE3
$SQL_QUERY3
EOF