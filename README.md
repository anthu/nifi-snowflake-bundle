# Nifi Snowflake Bundle
This is an unofficial Nifi Snowflake bundle.

## Processors
### PutSnowflakeStreamIngest
This processor takes a structured Flow File and ingests it to a Snowflake Table.
The table needs to be prepared in Snowflake and match the schema of the FlowFiles.
#### Configuration

| Name                         | Required | Description                                                                           | Example                          |
|------------------------------|----------|---------------------------------------------------------------------------------------|----------------------------------|
| Record Reader                | true     | Instance of a Record Reader for FlowFile parsing and schema extraction                | CSVReader                        |
| Snowflake Connection Service | true     | Instance of Snowflake Ingest Controller Service which connects to a Snowflake account | SnowflakeIngestControllerService |
| Snowflake Database           | true     | Existing Database to ingest records to                                                | `PLAYGROUND_DB`                  |
| Snowflake Schema             | true     | Existing Schema to ingest records                                                     | `PUBLIC`                         |
| Snowflake Table              | true     | Existing Table with matching FlowFile Schema acting as a record sink                  | `STREAM_INGEST_SINK`             |
| Snowflake Channel Name       | true     | Channel Name for ingestion                                                            | `mychannel42`                    |
#### Relationships
| Name    | Description                      |
|---------|----------------------------------|
| success | Successfully processed FlowFiles |
| failure | Failed FlowFiles                 |

## Controller Services
### SnowflakeIngestControllerService
Shared Snowflake connection used to provide channels and abstract the SDK
#### Configuration
| Name               | Required | Description                                                           | Example                                            |
|--------------------|----------|-----------------------------------------------------------------------|----------------------------------------------------|
| Snowflake URL      | true     | Account URL. Protocol and port are optional                           | `xy123456.europe-west4.gcp.snowflakecomputing.com` |
| Snowflake User     | true     | Username                                                              | `notadmin`                                         |
| Snowflake Password | true     | Password for the defined User                                         | `this_is_n0t_a_safe_password`                      |
| Snowflake Role     | false    | Role to use for ingestion. User default role will be used if not set. | `INGEST_ROLE`                                      |