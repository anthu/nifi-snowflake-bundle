# Nifi Snowflake Bundle
This is an unofficial Nifi Snowflake bundle for Stream Ingest (in Private Preview). 
It uses the Snowflake Ingest Java SDK provided at [snowflakedb/snowflake-ingest-java](https://github.com/snowflakedb/snowflake-ingest-java).


## Processors
### PutSnowflakeStreamIngest
This processor takes a structured Flow File and ingests it to a Snowflake Table. 
The table needs to be prepared in Snowflake and match the schema of the FlowFiles.

> **Note**
> Currently the Record Schema is treated case-sensitive

#### Configuration

| Name                            | Required | Description                                                                           | Example                          |
|---------------------------------|----------|---------------------------------------------------------------------------------------|----------------------------------|
| Record Reader                   | true     | Instance of a Record Reader for FlowFile parsing and schema extraction                | CSVReader                        |
| Snowflake Connection Service    | true     | Instance of Snowflake Ingest Controller Service which connects to a Snowflake account | SnowflakeIngestControllerService |
| Snowflake Database              | true     | Existing Database to ingest records to                                                | `PLAYGROUND_DB`                  |
| Snowflake Schema                | true     | Existing Schema to ingest records                                                     | `PUBLIC`                         |
| Snowflake Table                 | true     | Existing Table with matching FlowFile Schema acting as a record sink                  | `STREAM_INGEST_SINK`             |
| Snowflake Channel Name          | true     | Channel Name for ingestion                                                            | `mychannel42`                    |
| Add ingestion timestamp         | true     | Set to true if the Processor should write the ingestion timestamp                     | `true`                           |
| Ingestion Timestamp Column Name | true     | Column Name for timestamp. Only required if "Add ingestion timestamp" is set to true  | `INGESTED_AT`                    |

#### Relationships
| Name    | Description                      |
|---------|----------------------------------|
| success | Successfully processed FlowFiles |
| failure | Failed FlowFiles                 |

### PutSnowflakeStreamIngestAsVariant
This processor takes a structured Flow File and ingests it to a Snowflake column as Variant.
The table needs to be prepared in Snowflake. Please mind the limits of a Variant field for Snowflake
#### Configuration

| Name                            | Required | Description                                                                           | Example                          |
|---------------------------------|----------|---------------------------------------------------------------------------------------|----------------------------------|
| Record Reader                   | true     | Instance of a Record Reader for FlowFile parsing and schema extraction                | CSVReader                        |
| Snowflake Connection Service    | true     | Instance of Snowflake Ingest Controller Service which connects to a Snowflake account | SnowflakeIngestControllerService |
| Snowflake Database              | true     | Existing Database to ingest records to                                                | `PLAYGROUND_DB`                  |
| Snowflake Schema                | true     | Existing Schema to ingest records                                                     | `PUBLIC`                         |
| Snowflake Table                 | true     | Existing Table with matching FlowFile Schema acting as a record sink                  | `STREAM_INGEST_SINK`             |
| Snowflake Channel Name          | true     | Channel Name for ingestion                                                            | `mychannel42`                    |
| Snowflake Target Column         | true     | Target column for the record data                                                     | `V`                              |
| Add ingestion timestamp         | true     | Set to true if the Processor should write the ingestion timestamp                     | `true`                           |
| Ingestion Timestamp Column Name | true     | Column Name for timestamp. Only required if "Add ingestion timestamp" is set to true  | `INGESTED_AT`                    |

#### Relationships
| Name    | Description                      |
|---------|----------------------------------|
| success | Successfully processed FlowFiles |
| failure | Failed FlowFiles                 |

## Controller Services
### SnowflakeIngestControllerService
Shared Snowflake connection used to provide channels and abstract the SDK
#### Configuration
| Name                  | Required | Description                                                           | Example                                            |
|-----------------------|----------|-----------------------------------------------------------------------|----------------------------------------------------|
| Snowflake URL         | true     | Account URL. Protocol and port are optional                           | `xy123456.europe-west4.gcp.snowflakecomputing.com` |
| Snowflake User        | true     | Snowflake Username                                                    | `notadmin`                                         |
| Snowflake private key | true     | Private Key for User. See [Using Key Pair Authentication & Key Rotation](https://docs.snowflake.com/en/user-guide/kafka-connector-install.html#using-key-pair-authentication-key-rotation) for generation and usage example. | `-----BEGIN PRIVATE KEY-----MIIEvwIBADANBgk...`    |
| Snowflake Role        | false    | Role to use for ingestion. User default role will be used if not set. | `INGEST_ROLE`                                      |