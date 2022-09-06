package dev.anthu.processors.snowflake;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class SnowflakeDefaultProperties {


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
}
