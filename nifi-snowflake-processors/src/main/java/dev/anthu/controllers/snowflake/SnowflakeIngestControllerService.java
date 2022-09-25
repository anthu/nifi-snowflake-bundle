package dev.anthu.controllers.snowflake;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SnowflakeIngestControllerService extends AbstractControllerService implements SnowflakeIngestController {

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

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SNOWFLAKE_URL);
        properties.add(SNOWFLAKE_USER);
        properties.add(SNOWFLAKE_PRIVATE_KEY);
        properties.add(SNOWFLAKE_ROLE);
        return properties;
    }

    private SnowflakeStreamingIngestClient client;
    private final Map<String,SnowflakeStreamingIngestChannel> channelMap = new HashMap<>();

    public SnowflakeStreamingIngestChannel getChannel(String database, String schema, String table, String channelName) {
        SnowflakeStreamingIngestChannel currentChannel = channelMap.get(database + "." + schema + "." + table + ":" + channelName);
        if (currentChannel == null)
        {
            try {
                OpenChannelRequest channelRequest = OpenChannelRequest.builder(UUID.randomUUID().toString())
                        .setDBName(database)
                        .setSchemaName(schema)
                        .setTableName(table)
                        .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                        .build();

                currentChannel = client.openChannel(channelRequest);
                channelMap.put(database + "." + schema + "." + table + ":" + channelName, currentChannel);
            } catch (Exception e) {
                getLogger().error("Unable to establish Snowflake channel: {}\n{}", channelName, e.toString());
            }
        }
        return currentChannel;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        Properties props = new Properties();
        props.setProperty("url", context.getProperty(SNOWFLAKE_URL).getValue());
        props.setProperty("user", context.getProperty(SNOWFLAKE_USER).getValue());
        props.setProperty("private_key", context.getProperty(SNOWFLAKE_PRIVATE_KEY).getValue());
        if (context.getProperty(SNOWFLAKE_ROLE).isSet()) {
            props.setProperty("role", context.getProperty(SNOWFLAKE_ROLE).getValue());
        }

        client = SnowflakeStreamingIngestClientFactory
                .builder("NIFI")
                .setProperties(props)
                .build();
    }

    public void closeChannel(String database, String schema, String table, String channelName) {
        SnowflakeStreamingIngestChannel currentChannel = channelMap.remove(database + "." + schema + "." + table + ":" + channelName);
        currentChannel.close();
    }
}
