/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.sink.kite;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.security.UserGroupInformation;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.DatasetWriterException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;

/**
 * Sink that writes events to a Kite Dataset. This sink will parse the body of
 * each incoming event and store the resulting entity in a Kite Dataset. It
 * determines datasetUri Dataset by opening a dataset URI
 * {@code kite.dataset.uri} or opening a repository URI, {@code kite.repo.uri},
 * and loading a Dataset by name, {@code kite.dataset.name}.
 */
public class DatasetSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetSink.class);

  private Context context = null;
  private UserGroupInformation login = null;

  private String datasetName = null;
  private URI datasetUri = null;
  private Schema datasetSchema = null;
  private DatasetWriter<GenericRecord> writer = null;

  /**
   * The number of events to process as a single batch.
   */
  private long batchSize = DEFAULT_BATCH_SIZE;

  /**
   * The number of seconds to wait before rolling a writer.
   */
  private int rollIntervalS = DEFAULT_ROLL_INTERVAL;

  /**
   * Flag that says if Flume should commit on every batch.
   */
  private boolean commitOnBatch = DEFAULT_COMMIT_ON_BATCH;

  /**
   * The last time the writer rolled.
   */
  private long lastRolledMs = 0l;

  /**
   * The raw number of bytes parsed.
   */
  private long bytesParsed = 0l;

  /**
   * A class for parsing Kite entities from Flume Events.
   */
  private EntityParser<GenericRecord> parser = null;

  /**
   * A class implementing a failure policy for events that had a
   * non-recoverable error during processing.
   */
  private FailurePolicy policy = null;

  private SinkCounter counter = null;

  /**
   * The Kite entity
   */
  private GenericRecord entity = null;
  // TODO: remove this after PARQUET-62 is released
  private boolean reuseEntity = true;

  /**
   * The Flume transaction. Used to keep transactions open across calls to
   * process.
   */
  private Transaction transaction = null;

  /**
   * Return the list of allowed formats.
   * @return The list of allowed formats.
   */
  protected List<String> allowedFormats() {
    return Lists.newArrayList("avro", "parquet");
  }

  @Override
  public void configure(Context context) {
    this.context = context;

    // initialize login credentials
    this.login = KerberosUtil.login(
        context.getString(AUTH_PRINCIPAL),
        context.getString(AUTH_KEYTAB));
    String effectiveUser
        = context.getString(AUTH_PROXY_USER);
    if (effectiveUser != null) {
      this.login = KerberosUtil.proxyAs(effectiveUser, login);
    }

    // Get the dataset URI and name from the context
    String datasetURI = context.getString(CONFIG_KITE_DATASET_URI);
    if (datasetURI != null) {
      this.datasetUri = URI.create(datasetURI);
      this.datasetName = uriToName(datasetUri);
    } else {
      String repositoryURI = context.getString(CONFIG_KITE_REPO_URI);
      Preconditions.checkNotNull(repositoryURI, "Repository URI is missing."
          + " You must set either " + CONFIG_KITE_DATASET_URI + " or both"
          + " " + CONFIG_KITE_REPO_URI + " and " + CONFIG_KITE_DATASET_NAME);

      this.datasetName = context.getString(CONFIG_KITE_DATASET_NAME);
      Preconditions.checkNotNull(datasetName, "Dataset name is missing."
          + " You must set either " + CONFIG_KITE_DATASET_URI + " or both"
          + " " + CONFIG_KITE_REPO_URI + " and " + CONFIG_KITE_DATASET_NAME);

      String namespace = context.getString(CONFIG_KITE_DATASET_NAMESPACE,
          DEFAULT_NAMESPACE);

      this.datasetUri = new URIBuilder(repositoryURI, namespace, datasetName)
          .build();
    }
    this.setName(datasetUri.toString());

    // Create the configured failure policy
    String policyName = context.getString(CONFIG_FAILURE_POLICY,
        DEFAULT_FAILURE_POLICY);
    if (policyName.equals(RETRY_FAILURE_POLICY)) {
      this.policy = new RetryPolicy();
    } else if (policyName.equals(SAVE_FAILURE_POLICY)) {
      this.policy = new SavePolicy(context);
    } else {
      throw new IllegalArgumentException("Invalid failure policy '" + policyName 
          + "'. Available policies: " + Arrays.toString(AVAILABLE_POLICIES));
    }

    // other configuration
    this.batchSize = context.getLong(CONFIG_KITE_BATCH_SIZE,
        DEFAULT_BATCH_SIZE);
    this.rollIntervalS = context.getInteger(CONFIG_KITE_ROLL_INTERVAL,
        DEFAULT_ROLL_INTERVAL);

    this.counter = new SinkCounter(datasetName);
  }

  @Override
  public synchronized void start() {
    lastRolledMs = System.currentTimeMillis();
    counter.start();
    // signal that this sink is ready to process
    LOG.info("Started DatasetSink " + getName());
    super.start();
  }

  /**
   * Causes the sink to roll at the next {@link #process()} call.
   */
  @VisibleForTesting
  public void roll() {
    lastRolledMs = 0l;
  }

  @Override
  public synchronized void stop() {
    counter.stop();

    if (writer != null) {
      try {
        // Close the writer and commit the transaction,
        // but don't create a new writer
        rollWriter(true, false);
      } catch (EventDeliveryException ex) {
        LOG.warn("Rolling the writer failed: " + ex.getLocalizedMessage());
        LOG.debug("Exception follows.", ex);
        // We don't propogate the exception as the transaction would have been
        // rolled back and we can still finish stopping
      }
    }

    // signal that this sink has stopped
    LOG.info("Stopped dataset sink: " + getName());
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    if (writer == null) {
      this.writer = newWriter();
    }

    if (shouldRoll()) {
      rollWriter(true, true);
    }

    Channel channel = getChannel();
    try {
      long processedEvents = 0;

      // TODO: Can the same Sink object be called from multiple threads
      if (transaction == null) {
        transaction = channel.getTransaction();
        transaction.begin();
      }

      for (; processedEvents < batchSize; processedEvents += 1) {
        Event event = channel.take();
        
        if (event == null) {
          // no events available in the channel
          break;
        }
        
        write(event);
      }

      // commit transaction
      if (commitOnBatch) {
        // End the batch with the failure policy first. If this fails, we'll
        // end up rolling back the transaction, so we don't need to do the sync
        policy.endBatch();

        // Sync before commiting. A failure here will result in rolling back
        // the transaction
        writer.sync();
        transaction.commit();
        transaction.close();
        transaction = null;
      }

      if (processedEvents == 0) {
        counter.incrementBatchEmptyCount();
        return Status.BACKOFF;
      } else if (processedEvents < batchSize) {
        counter.incrementBatchUnderflowCount();
      } else {
        counter.incrementBatchCompleteCount();
      }

      counter.addToEventDrainSuccessCount(processedEvents);

      return Status.READY;

    } catch (Throwable th) {
      // catch-all for any unhandled Throwable so that the transaction is
      // correctly rolled back.
      if (transaction != null) {
        try {
          transaction.rollback();
          transaction.close();
        } catch (Exception ex) {
          LOG.error("Transaction rollback failed: " + ex.getLocalizedMessage());
          LOG.debug("Exception follows.", ex);
          // We don't propogate the exception as the original throwable will
          // be propogated later.
        } finally {
          transaction = null;
        }
      }

      // We don't want to close the writer because that will persist data 
      // that will be replayed. This will avoid duplicate data.
      // This is also no different the the no-op close the writer does if
      // it's already in the ERROR state.
      try {
        rollWriter(false, true);
      } catch (EventDeliveryException ex) {
        LOG.warn("Rolling the writer failed, will retry: "
            + ex.getLocalizedMessage());
        LOG.debug("Exception follows.", ex);
        // We don't propogate the exception as the original throwable will
        // be propogated later.
      }

      // handle the exception
      Throwables.propagateIfInstanceOf(th, Error.class);
      Throwables.propagateIfInstanceOf(th, EventDeliveryException.class);
      throw new EventDeliveryException(th);
    }
  }

  /**
   * Parse the event using the entity parser and write the entity to the dataset.
   * 
   * @param event The event to write
   * @throws EventDeliveryException An error occurred trying to write to the
   *                                dataset that couldn't or shouldn't be
   *                                handled by the failure policy.
   */
  private void write(Event event) throws EventDeliveryException {
    try {
      entity = parser.parse(event, reuseEntity ? entity : null);
      bytesParsed += event.getBody().length;

      // writeEncoded would be an optimization in some cases, but HBase
      // will not support it and partitioned Datasets need to get partition
      // info from the entity Object. We may be able to avoid the
      // serialization round-trip otherwise.
      writer.write(entity);
    } catch (NonRecoverableEventException ex) {
      policy.handle(event, ex);
    } catch (DataFileWriter.AppendWriteException ex) {
      policy.handle(event, ex);
    } catch (RuntimeException ex) {
      Throwables.propagateIfInstanceOf(ex, EventDeliveryException.class);
      throw new EventDeliveryException(ex);
    }
  }

  /**
   * Create a new writer.
   * 
   * This method also re-loads the dataset so updates to the configuration or
   * a dataset created after Flume starts will be loaded.
   * 
   * @return The dataset writer
   * @throws EventDeliveryException There was an error creating the writer.
   */
  private DatasetWriter<GenericRecord> newWriter()
      throws EventDeliveryException {
    try {
      View<GenericRecord> view = KerberosUtil.runPrivileged(login,
          new PrivilegedExceptionAction<Dataset<GenericRecord>>() {
            @Override
            public Dataset<GenericRecord> run() {
              return Datasets.load(datasetUri);
            }
          });

      DatasetDescriptor descriptor = view.getDataset().getDescriptor();
      String formatName = descriptor.getFormat().getName();
      Preconditions.checkArgument(allowedFormats().contains(formatName),
          "Unsupported format: " + formatName);

      Schema newSchema = descriptor.getSchema();
      if (datasetSchema == null || !newSchema.equals(datasetSchema)) {
        this.datasetSchema = descriptor.getSchema();
        // dataset schema has changed, create a new parser
        parser = newParser();
      }

      this.reuseEntity = !("parquet".equals(formatName));
      // Commit on batch if kite.commitOnBatch is true and the format isn't
      // parquet
      this.commitOnBatch = context.getBoolean(CONFIG_COMMIT_ON_BATCH,
          DEFAULT_COMMIT_ON_BATCH) && !("parquet".equals(formatName));
      this.datasetName = view.getDataset().getName();

      return view.newWriter();
    } catch (DatasetNotFoundException ex) {
      throw new EventDeliveryException("Dataset " + datasetUri + " not found."
          + " The dataset must be created before Flume can write to it.", ex);
    } catch (RuntimeException ex) {
      throw new EventDeliveryException("Error trying to open a new"
          + " writer for dataset " + datasetUri + ": "
          + ex.getLocalizedMessage(), ex);
    }
  }

  /**
   * Return true if the sink should roll the writer.
   * 
   * Currently, this is based on time since the last roll.
   * 
   * @return True if and only if the sink should roll the writer
   */
  private boolean shouldRoll() {
    long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(
        System.currentTimeMillis() - lastRolledMs);

    return elapsedTimeSeconds > rollIntervalS;
  }

  /**
   * Roll the dataset writer. If there's an error during the roll, the
   * transaction will be rolled back.
   * 
   * @param closeWriter If true, the current writer is closed
   * @param createNewWriter If true, a new writer is created
   * @throws EventDeliveryException There was an error during the roll which
   *                                means we can't guarantee previously written
   *                                records were persisted.
   */
  private void rollWriter(boolean closeWriter, boolean createNewWriter)
      throws EventDeliveryException {
    try {
      // close the current writer and get a new one
      if (writer != null && closeWriter) {
        try {
          writer.close();
        } catch (DatasetIOException ex) {
          throw new EventDeliveryException("Check HDFS permissions/health. IO"
              + " error trying to close the  writer for dataset " + datasetUri
              + ": " + ex.getLocalizedMessage(), ex);
        } catch (DatasetWriterException ex) {
          throw new EventDeliveryException("Failure moving temp file: "
              + ex.getLocalizedMessage(), ex);
        } catch (RuntimeException ex) {
          throw new EventDeliveryException("Error trying to close the  writer for"
              + " dataset " + datasetUri + ": " + ex.getLocalizedMessage(), ex);
        }
      }
      writer = null;

      // commit transaction
      if (transaction != null) {
        policy.endBatch();
        transaction.commit();
        transaction.close();
        transaction = null;
      }

      if (createNewWriter) {
        writer = newWriter();
      }

      if (closeWriter) {
        // Only log if we closed the old writer
        long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(
            System.currentTimeMillis() - lastRolledMs);
        LOG.info("Rolled writer for {} after {} seconds and {} bytes parsed",
            new Object[]{datasetUri, elapsedTimeSeconds, bytesParsed});
      }
    } catch (Exception ex) {
      try {
        // If the transaction wasn't committed before we got the exception, we
        // need to rollback.
        if (transaction != null) {
          transaction.rollback();
          transaction.close();
          transaction = null;
        }
      } catch (RuntimeException rollbackException) {
        LOG.error("Transaction rollback failed: "
            + rollbackException.getLocalizedMessage());
        LOG.debug("Exception follows.", rollbackException);
      }

      Throwables.propagateIfInstanceOf(ex, EventDeliveryException.class);
      throw new EventDeliveryException("Failed to roll writer for " + datasetUri
          + ": " + ex.getLocalizedMessage(), ex);
    } finally {
      // The transaction was either committed or rolled back, either way make
      // sure that it's set to null so a new transaction will be retrieved from
      // the channel during process
      transaction = null;
      lastRolledMs = System.currentTimeMillis();
      bytesParsed = 0l;
    }
  }

  /**
   * Create a new entity parser
   * 
   * @return The entity parser
   */
  private EntityParser<GenericRecord> newParser() {
    String parserType = context.getString(CONFIG_ENTITY_PARSER,
        DEFAULT_ENTITY_PARSER);

    if (parserType.equals(AVRO_ENTITY_PARSER)) {
      return new AvroParser(datasetSchema);
    } else {
      throw new IllegalArgumentException("Invalid entity parser '" + parserType
          + "'. Available parsers: " + Arrays.toString(AVAILABLE_PARSERS));
    }
  }

  /**
   * Get the name of the dataset from the URI
   * 
   * @param uri The dataset or view URI
   * @return The dataset name
   */
  private static String uriToName(URI uri) {
    return Registration.lookupDatasetUri(URI.create(
        uri.getRawSchemeSpecificPart())).second().get("dataset");
  }
}
