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

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Tags({"snowflake", "stream"})
@CapabilityDescription("Write Record Wise to Snowflake stream")
@ReadsAttributes({@ReadsAttribute(attribute="")})
@WritesAttributes({@WritesAttribute(attribute="")})
public class StreamToSnowflakeTable extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INSERT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Insert Strategy")
            .description("Insert strategy for streaming ingest. Either flush row by row or a FlowFile as whole")
            .required(true)
            .allowableValues("row-by-row", "per FlowFile")
            .defaultValue("row-by-row")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .description("A Flowfile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A Flowfile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    private SnowflakeStreamingIngestChannel channel1;
    private boolean rowByRowInsertStrategy;

    @Override
    protected void init(final ProcessorInitializationContext context) {

    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        rowByRowInsertStrategy = context.getProperty(INSERT_STRATEGY).getValue().equals("row-by-row");

        Properties props = new Properties();
        props.setProperty("url", context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_URL).getValue());
        props.setProperty("user", context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_USER).getValue());
        props.setProperty("private_key", context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_PRIVATE_KEY).getValue());
        if(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_ROLE).isSet()) {
           props.setProperty("role", context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_ROLE).getValue());
        }

        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory
                .builder("NIFI")
                .setProperties(props)
                .build();

        OpenChannelRequest channelRequest = OpenChannelRequest.builder(UUID.randomUUID().toString())
                .setDBName(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE).getValue())
                .setSchemaName(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA).getValue())
                .setTableName(context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_TABLE).getValue())
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();

        channel1 = client.openChannel(channelRequest);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_URL);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_USER);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_PRIVATE_KEY);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_ROLE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_TABLE);
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
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final Map<String, String> originalAttributes = flowFile.getAttributes();
        Record record;
        List<Map<String, Object>> rows = new ArrayList<>();

        try (   final InputStream in = session.read(flowFile);
                final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, flowFile.getSize(), getLogger())
        ) {
             final RecordSchema recordSchema = reader.getSchema();
             while ((record = reader.nextRecord()) != null) {
                 Map<String, Object> row = new HashMap<>();
                 for (RecordField field : recordSchema.getFields()) {
                     Object recordValue = record.getValue(field.getFieldName());
                     row.put(field.getFieldName(), recordValue);
                     getLogger().info("Adding {} as {}", field.getFieldName(), recordValue);
                 }

                 if (rowByRowInsertStrategy) {
                     InsertValidationResponse response = channel1.insertRow(row, null);
                     if (response.hasErrors()) {
                         throw (response.getInsertErrors().get(0)).getException();
                     }
                 } else {
                     rows.add(row);
                 }
             }
            if(!rowByRowInsertStrategy) {
                InsertValidationResponse response = channel1.insertRows(rows, null);
                if (response.hasErrors()) {
                    throw (response.getInsertErrors().get(0)).getException();
                }
            }
        } catch (SchemaNotFoundException | IOException | MalformedRecordException e) {
            getLogger().error("Failed to deserialize {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
