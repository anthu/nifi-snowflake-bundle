package dev.anthu.controllers.snowflake;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.nifi.controller.ControllerService;

public interface SnowflakeIngestController extends ControllerService {
    SnowflakeStreamingIngestChannel getChannel(String database, String schema, String table, String channelName);

    void closeChannel(String database, String schema, String table, String channelName);
}
