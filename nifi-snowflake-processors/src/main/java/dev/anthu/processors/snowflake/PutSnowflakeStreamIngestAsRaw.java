/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.anthu.processors.snowflake;

import dev.anthu.controllers.snowflake.SnowflakeIngestController;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.SFException;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"snowflake", "stream"})
@CapabilityDescription("Write Raw FlowFile to Snowflake stream")
@ReadsAttributes({@ReadsAttribute(attribute = "")})
@WritesAttributes({@WritesAttribute(attribute = "")})
public class PutSnowflakeStreamIngestAsRaw extends AbstractProcessor {



    public static final PropertyDescriptor SNOWFLAKE_TARGET_COLUMN = new PropertyDescriptor.Builder()
            .name("Snowflake Target Column")
            .description("Snowflake Target Column to insert record")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("col1")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    private SnowflakeIngestController snowflakeController;

    private String database;
    private String schema;
    private String table;
    private String channelName;
    private String targetColumn;
    private boolean writeTimestamp;
    private String timestampTargetColumn;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        snowflakeController = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_SERVICE).asControllerService(SnowflakeIngestController.class);
        database = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE).getValue();
        schema = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA).getValue();
        table = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_TABLE).getValue();
        channelName = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_STREAMING_CHANNEL).getValue();
        targetColumn = context.getProperty(SNOWFLAKE_TARGET_COLUMN).getValue();
        writeTimestamp = Boolean.parseBoolean(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_ADD_INGESTION_TIMESTAMP).getValue());
        timestampTargetColumn = optionallyQuoteColumnName(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_INGESTION_TIMESTAMP_COLUMN).getValue());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_SERVICE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_TABLE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_STREAMING_CHANNEL);
        properties.add(SNOWFLAKE_TARGET_COLUMN);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_ADD_INGESTION_TIMESTAMP);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_INGESTION_TIMESTAMP_COLUMN);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try (final InputStream in = session.read(flowFile)) {
            SnowflakeStreamingIngestChannel channel1 = snowflakeController.getChannel(database, schema, table, channelName);

            Map<String, Object> row = new HashMap<>();
            String text = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            row.put(targetColumn, text);

            if(writeTimestamp) {
                row.put(timestampTargetColumn, LocalDateTime.now(ZoneOffset.UTC));
            }

            InsertValidationResponse response = channel1.insertRow(row, null);

            if (response.hasErrors()) {
                throw (response.getInsertErrors().get(0)).getException();
            }
        } catch (SFException e) {
            getLogger().error("Failed to insert Row.", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (IOException e) {
            getLogger().error("Failed write record {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private static String optionallyQuoteColumnName(String columnName) {
        if (columnName.equals(columnName.toUpperCase())) {
            return columnName;
        }
        return '"' + columnName + '"';
    }

    @OnShutdown
    public void cleanup() {
        snowflakeController.closeChannel(database, schema, table, channelName);
    }
}
