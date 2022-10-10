package dev.anthu.processors.snowflake;

import dev.anthu.controllers.snowflake.SnowflakeIngestController;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;

public class SnowflakeDefaultProperties {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor SNOWFLAKE_SERVICE = new PropertyDescriptor
            .Builder().name("Snowflake Connection Service")
            .description("Provides connection to Snowflake REST API")
            .required(true)
            .identifiesControllerService(SnowflakeIngestController.class)
            .build();

    static final PropertyDescriptor SNOWFLAKE_STREAMING_CHANNEL = new PropertyDescriptor.Builder()
            .name("Snowflake Channel Name")
            .description("Channel Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("channel1")
            .build();

    static final PropertyDescriptor SNOWFLAKE_DATABASE = new PropertyDescriptor.Builder()
            .name("snowflake-database")
            .displayName("Snowflake database")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SNOWFLAKE_SCHEMA = new PropertyDescriptor.Builder()
            .name("snowflake-schema")
            .displayName("Snowflake schema")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SNOWFLAKE_TABLE = new PropertyDescriptor.Builder()
            .name("snowflake-table")
            .displayName("Snowflake table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SNOWFLAKE_ADD_INGESTION_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("snowflake-add-ingestion-timestamp")
            .displayName("Add ingestion timestamp")
            .description("Should write the ingestion timestamp to a target column. In case of Variant target the timestamp will be written to a dedicated column.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor SNOWFLAKE_INGESTION_TIMESTAMP_COLUMN = new PropertyDescriptor.Builder()
            .name("snowflake-ingestion-timestamp-column")
            .displayName("Ingestion Timestamp Column Name")
            .description("Target column for the timestamp. This value has no effect if the \"Add ingestion timestamp\" property is disabled. This value is case-sensitive and will override any overlapping column name within the data.")
            .dependsOn(SNOWFLAKE_ADD_INGESTION_TIMESTAMP, "true")
            .required(true)
            .defaultValue("INGESTED_AT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
}
