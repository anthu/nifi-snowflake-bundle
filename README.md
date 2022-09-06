# Nifi Snowflake Bundle
This is an unofficial Nifi Snowflake bundle.

## Processors
### PutSnowflakeStreamIngest
This processor takes a structured Flow File and ingests it to a Snowflake Table.
The table needs to be prepared in Snowflake and match the schema of the FlowFiles.