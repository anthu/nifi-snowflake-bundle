package dev.anthu.processors.snowflake;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class SnowflakeDefaultProperties {
    static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor.Builder()
            .name("snowflake-url")
            .displayName("Snowflake URL")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SNOWFLAKE_USER = new PropertyDescriptor.Builder()
            .name("snowflake-user")
            .displayName("Snowflake User")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SNOWFLAKE_PRIVATE_KEY = new PropertyDescriptor.Builder()
            .name("snowflake-private-key")
            .displayName("Snowflake private key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SNOWFLAKE_ROLE = new PropertyDescriptor.Builder()
            .name("snowflake-role")
            .displayName("Snowflake role")
            .required(false)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
}
