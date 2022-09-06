FROM apache/nifi:latest
ADD nifi-snowflake-nar/target/*.nar /opt/nifi/nifi-current/lib