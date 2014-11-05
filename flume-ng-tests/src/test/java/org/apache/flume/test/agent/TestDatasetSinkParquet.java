/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.test.agent;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flume.test.util.StagedInstall;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.minicluster.FlumeService;
import org.kitesdk.minicluster.HdfsService;
import org.kitesdk.minicluster.HiveService;
import org.kitesdk.minicluster.MiniCluster;

public class TestDatasetSinkParquet {

  private static final Logger LOGGER = Logger.getLogger(TestDatasetSinkParquet.class);

  private static final Collection<File> tempResources = new ArrayList<File>();

  private Properties agentProps;
  private String datasetUri;
  private MiniCluster miniCluster;
  private Schema schema;

  @Before
  public void setUp() throws Exception {

    schema = new Schema.Parser().parse(
        Resources.getResource("event.avsc").openStream());

    datasetUri = "dataset:hdfs:/datasets/default/events";

    /* Create 2 temp dirs, each used as value within agentProps */

    final File channelCheckpointDir = Files.createTempDir();
    tempResources.add(channelCheckpointDir);
    final String channelCheckpointDirPath = channelCheckpointDir
        .getCanonicalPath();
    LOGGER.info("Created file channel's checkpoint dir: "
        + channelCheckpointDirPath);

    final File channelDataDir = Files.createTempDir();
    tempResources.add(channelDataDir);
    final String channelDataDirPath = channelDataDir.getCanonicalPath();
    LOGGER.info("Created file channel's data dir: "
        + channelDataDirPath);

    /* Build props to pass to flume agent */
    agentProps = new Properties();

    agentProps.put("a1.sources", "avro-event-source");
    agentProps.put("a1.channels", "avro-event-channel");
    agentProps.put("a1.sinks", "kite-dataset");

    agentProps.put("a1.sources.avro-event-source.type", "avro");
    agentProps.put("a1.sources.avro-event-source.channels", "avro-event-channel");
    agentProps.put("a1.sources.avro-event-source.bind", "0.0.0.0");
    agentProps.put("a1.sources.avro-event-source.port", "41415");

    agentProps.put("a1.channels.avro-event-channel.type", "file");
    agentProps.put("a1.channels.avro-event-channel.capacity", "18000000");
    agentProps.put("a1.channels.avro-event-channel.transactionCapacity", "3000000");
    agentProps.put("a1.channels.avro-event-channel.maxFileSize", "3073741824");
    agentProps.put("a1.channels.avro-event-channel.minimumRequiredSpace", "10442450944");
    agentProps.put("a1.channels.avro-event-channel.checkpointDir", channelCheckpointDirPath);
    agentProps.put("a1.channels.avro-event-channel.dataDirs", channelDataDirPath);

    agentProps.put("a1.sinks.kite-dataset.type", "org.apache.flume.sink.kite.DatasetSink");
    agentProps.put("a1.sinks.kite-dataset.channel", "avro-event-channel");
    agentProps.put("a1.sinks.kite-dataset.kite.dataset.uri", datasetUri);
    agentProps.put("a1.sinks.kite-dataset.kite.batchSize", "1000");
    agentProps.put("a1.sinks.kite-dataset.kite.rollInterval", "60");
    agentProps.put("a1.sinks.kite-dataset.kite.rollSize", "268435456");

    final File flumeConfFile = File.createTempFile("flume", "conf");
    final FileOutputStream flumeConfOut = new FileOutputStream(flumeConfFile);
    tempResources.add(flumeConfFile);
    agentProps.store(flumeConfOut, "");
    flumeConfOut.close();

    final File miniClusterDir = Files.createTempDir();
    tempResources.add(miniClusterDir);
    miniCluster = new MiniCluster.Builder()
        .workDir(miniClusterDir.getCanonicalPath())
        .flumeAgentName("a1")
        .flumeConfiguration(flumeConfFile.getCanonicalPath())
        .bindIP("127.0.0.1")
        .addService(HdfsService.class)
        .addService(FlumeService.class)
        .addService(HiveService.class) // Remove this after Kite mini cluster makes it optional
        .clean(true).build();

    miniCluster.start();

    Datasets.create(datasetUri, new DatasetDescriptor.Builder()
        .schema(schema)
        .format(Formats.PARQUET)
        .build());
    LOGGER.info("Created dataset sink's destination dataset: "
        + datasetUri);

  }

  @After
  public void tearDown() throws Exception {
    try {
      miniCluster.stop();
    } finally {
      for (File tempResource : tempResources) {
        tempResource.delete();
      }
      agentProps = null;
    }
  }

  /**
   * Verifies that data will be rolled into the Dataset after a 60 second timeout
   * @throws Exception
   */
  @Test
  public void testTimeBasedRoll() throws Exception {
    LOGGER.debug("testTimeBasedRoll() started.");

    List<GenericRecord> data = generateData();
    Map<String, String> headers = ImmutableMap.of(
        "flume.avro.schema.url", "hdfs://localhost/datasets/default/events/.metadata/schema.avsc"
    );

    StagedInstall.waitUntilPortOpens("localhost", 41415, 20000);
    RpcClient client = RpcClientFactory.getDefaultInstance("localhost", 41415);
    BinaryEncoder encoder = null;

    for (GenericRecord record : data) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
      encoder = EncoderFactory.get().binaryEncoder(out, encoder);
      writer.write(record, encoder);
      encoder.flush();

      client.append(EventBuilder.withBody(
          out.toByteArray(), headers));
    }

    TimeUnit.SECONDS.sleep(60); // Wait for roll

    List<GenericRecord> actual = Lists.newLinkedList();

    Dataset<GenericRecord> dataset = Datasets.load(datasetUri).getDataset();
    DatasetReader<GenericRecord> reader = dataset.newReader();
    for (GenericRecord record : reader) {
      actual.add(record);
    }

    Assert.assertEquals("Records don't match", data, actual);

    LOGGER.debug("testTimeBasedRoll() ended.");
  }

  public List<GenericRecord> generateData() {
    List<GenericRecord> records = Lists.newLinkedList();
    for (int i = 0; i < 1000; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("payload", Integer.toString(i));
      record.put("timestamp", System.currentTimeMillis());
      records.add(record);
    }

    return records;
  }

}
