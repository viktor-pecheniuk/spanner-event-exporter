/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.spez;

import static com.google.cloud.spanner.TransactionRunner.TransactionCallable;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.Immutable;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Wraps the static factory functions for the Spanner Queues. */
@Immutable
public final class Queue {

  private Queue() {}

  /**
   * Creates the Spanner table the represents the Queue and stores the queue messages.
   *
   * @param dbAdminClient the Spanner admin client
   * @param id the Spanner database id
   * @param queueName the name of the queue
   */
  public static void createQueue(DatabaseAdminClient dbAdminClient, DatabaseId id, String queueName)
      throws SpannerException {
    final OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Arrays.asList(
                "CREATE TABLE "
                    + queueName
                    + "_queue "
                    + " (\n"
                    + "  MessageId STRING(36) NOT NULL,\n"
                    + "  Key       STRING(1024) NOT NULL,\n"
                    + "  Payload   BYTES(MAX) NOT NULL,\n"
                    + "  Ack       BOOL NOT NULL,\n"
                    + "  Timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n"
                    + ") PRIMARY KEY (MessageId)"));
    try {
      // Initiate the request which returns an OperationFuture.
      final Database db = op.get();
      System.out.println("Created database [" + db.getId() + "]");
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  /**
   * Sends a QueueMessage to the spanner queue table
   *
   * <p>Example of sending data to a queue.
   *
   * <pre>{@code
   * MyProto myProto = MyProto.newBuilder().setMessage("My-Message").build();
   *
   * try {
   *   Queue.send(dbClient, queueName, "myKey", ByteArray.copyFrom(myProto.toByteArray()));
   * } catch (SpannerException e) {
   *   log.error("Could not write message to Queue", e);
   * }
   * }</pre>
   *
   * @param dbClient the Spanner database client
   * @param queueName the name of the queue to be polled
   * @param key the name used to partition the passed value for storage and lookup
   * @param value the message payload
   * @return Timestamp the timestamp that the message was written
   */
  public static Timestamp send(DatabaseClient dbClient, String queueName, String key, byte[] value)
      throws SpannerException {
    Preconditions.checkNotNull(dbClient);
    Preconditions.checkNotNull(queueName);
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    final List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(queueName)
            .set("MessageId")
            .to(UUID.randomUUID().toString())
            .set("Key")
            .to(key)
            .set("Payload")
            .to(ByteArray.copyFrom(value))
            .set("Ack")
            .to(false)
            .set("Timestamp")
            .to(Value.COMMIT_TIMESTAMP)
            .build());

    return dbClient.write(mutations);
  }

  /**
   * Receivs messages from the queue and registers a {@link QueueMessageCallback} to process all
   * recieved messags.
   *
   * <p>This function will start a polling process to be run on a seperate thread and poll the
   * spanner table at a fixed polling interval. It will also register the provided call back to be
   * called in another thread to process the batch of {@link QueueMessage} it receives for each
   * poll. The poller will always pass back all messages that have not been ack'd at the time of
   * polling.
   *
   * <p>Example of registering a receiver and creating the callback
   *
   * <pre>{@code
   * // Create the Async processing callback
   * QueueMessageCallback cb = messages -> {
   *   messages.forEach(m -> {
   *   Optional<MyProto> myProto = Optional.of(MyProto.parseFrom(m.value().asInputStream()));
   *   // ... Work with message
   *   Queue.ack(dbClient, m);
   *  });
   * }
   *
   * // Register the receiver
   * Queue.recieve(dbClient, queueName, 20, 500, cb);
   * }</pre>
   *
   * @param dbClient the Spanner database client
   * @param queueName the name of the queue to be polled
   * @param recordLimit the maximum nuber of records to be returned in a single poll
   * @param pollRate the number of milliseconds to delay each poll
   * @param cb the function that each processed {@link QueueMessage} would be passed to
   */
  public static void receive(
      DatabaseClient dbClient,
      String queueName,
      String recordLimit,
      int pollRate,
      QueueMessageCallback cb) {
    Preconditions.checkNotNull(dbClient);
    Preconditions.checkNotNull(queueName);
    Preconditions.checkNotNull(recordLimit);
    Preconditions.checkArgument(pollRate > 100, "Poll Rate must be greater than 100ms");
    Preconditions.checkNotNull(cb);
    final String[] ts = new String[1];

    // Add hook to gracefully shutdown the spanner lib
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println(
                    "*** shutting down Queue receive poller since JVM is shutting down");

                System.err.println("*** service shut down");
              }
            });

    // Schedule our poller on a fixed rate to make sure we have a constant delay
    final ExecutorService service =
        Executors.newFixedThreadPool(
            12, new ThreadFactoryBuilder().setNameFormat("QueueMessage Callback Thread").build());
    final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(
            4, new ThreadFactoryBuilder().setNameFormat("Queue Poller Thread").build());
    scheduler.scheduleAtFixedRate(
        () -> {
          final Statement pollQuery =
              Statement.newBuilder(
                      "SELECT * FROM "
                          + queueName
                          + " WHERE Ack IS FALSE AND "
                          + " WHERE Timestamp > '"
                          + ts[0]
                          + "'"
                          + " ORDER BY Timestamp ASC LIMIT "
                          + recordLimit)
                  .build();

          final ResultSet resultSet = dbClient.readOnlyTransaction().executeQuery(pollQuery);
          final List<QueueMessage> messages = new ArrayList<>();

          while (resultSet.next()) {
            final Timestamp timestamp = resultSet.getTimestamp("Timestamp");
            messages.add(
                QueueMessage.create(
                    queueName,
                    resultSet.getString("MessageID"),
                    resultSet.getString("Key"),
                    resultSet.getBytes("Payload"),
                    timestamp));

            ts[0] = timestamp.toString();
          }

          service.submit(
              () -> {
                cb.process(messages);
              });
        },
        0,
        pollRate,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Acknowledges the {@link QueueMessage} and removes it from the processing Queue.
   *
   * @param dbClient the Spanner database client
   * @param message the message to acknowledge
   */
  public static void ack(DatabaseClient dbClient, QueueMessage message) {
    Preconditions.checkNotNull(dbClient);
    Preconditions.checkNotNull(message);

    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Override
              public Void run(TransactionContext transaction) throws Exception {
                String sql =
                    "UPDATE "
                        + message.queueName()
                        + " SET Ack = true"
                        + " WHERE MessageId = "
                        + message.id();
                final long rowCount = transaction.executeUpdate(Statement.of(sql));
                Preconditions.checkArgument(
                    rowCount == 1,
                    "Ack function acknowledged too many messages on a single commit");

                return null;
              }
            });
  }
}
