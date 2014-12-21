/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.kite;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

/**
 * A policy for dealing with non-recoverable event delivery failures.
 * 
 * Non-recoverable event delivery failures include:
 * 
 * 1. Error parsing the event body thrown from the {@link EntityParser}
 * 2. A schema mismatch between the schema of an event and the schema of the
 *    destination dataset.
 * 3. A missing schema from the Event header when using the
 *    {@link AvroEntityParser}.
 */
public interface FailurePolicy {

  /**
   * Handle a non-recoverable event.
   * 
   * @param event The event
   * @param cause The cause of the failure
   * @throws EventDeliveryException The policy failed to handle the event. When
   *                                this is thrown, the Flume transaction will
   *                                be rolled back and the event will be retried
   *                                along with the rest of the batch.
   */
  public void handle(Event event, Throwable cause)
      throws EventDeliveryException;

  /**
   * Called before the Flume transaction is committed.
   * 
   * This allows the policy implementation to flush any data that it may not
   * have fully handled.
   * 
   * @throws EventDeliveryException The policy failed while ending the batch.
   *                                When this is thrown, the Flume transaction
   *                                will be rolled back and the batch will be
   *                                retried.
   */
  public void endBatch() throws EventDeliveryException;
  
}
