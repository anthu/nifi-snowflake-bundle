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

import java.sql.Date;
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
import java.util.Set;

@Tags({"snowflake", "stream"})
@CapabilityDescription("Write Record Wise to Snowflake stream")
@ReadsAttributes({@ReadsAttribute(attribute = "")})
@WritesAttributes({@WritesAttribute(attribute = "")})
public class PutSnowflakeStreamIngest extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    private RecordReaderFactory readerFactory;
    private SnowflakeIngestController snowflakeController;

    private String database;
    private String schema;
    private String table;
    private String channelName;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        snowflakeController = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_SERVICE).asControllerService(SnowflakeIngestController.class);
        readerFactory = context.getProperty(SnowflakeDefaultProperties.RECORD_READER).asControllerService(RecordReaderFactory.class);
        database = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE).getValue();
        schema = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA).getValue();
        table = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_TABLE).getValue();
        channelName = context.getProperty(SnowflakeDefaultProperties.SNOWFLAKE_STREAMING_CHANNEL).getValue();
        getLogger().info("Initialized");
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SnowflakeDefaultProperties.RECORD_READER);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_SERVICE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_DATABASE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_SCHEMA);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_TABLE);
        properties.add(SnowflakeDefaultProperties.SNOWFLAKE_STREAMING_CHANNEL);
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

        final Map<String, String> originalAttributes = flowFile.getAttributes();

        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, flowFile.getSize(), getLogger())
        ) {
            SnowflakeStreamingIngestChannel channel1 = snowflakeController.getChannel(database, schema, table, channelName);

            final RecordSchema recordSchema = reader.getSchema();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                Map<String, Object> row = new HashMap<>();
                for (RecordField field : recordSchema.getFields()) {
                    String fieldName = field.getFieldName();
                    Object recordValue = record.getValue(fieldName);

                    // Stream Ingest rejects Date records -> casting to LocalDate
                    if (recordValue instanceof Date) {
                        recordValue = ((Date)recordValue).toLocalDate();
                    }

                    // Fix for optionally Quoted fields
                    if (fieldName.equals(fieldName.toUpperCase())) {
                        row.put(fieldName, recordValue);
                    } else {
                        row.put('"' + fieldName + '"', recordValue);
                    }
                }

                InsertValidationResponse response = channel1.insertRow(row, null);
                if (response.hasErrors()) {
                    throw (response.getInsertErrors().get(0)).getException();
                }
            }
            getLogger().info("All records done.");
        } catch (SchemaNotFoundException e) {
            getLogger().error("Failed to deserialize {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (SFException e) {
            getLogger().error("Failed to insert Row.", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (IOException | MalformedRecordException e) {
            getLogger().error("Failed write record {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    @OnShutdown
    public void cleanup() {
        snowflakeController.closeChannel(database, schema, table, channelName);
    }
}
