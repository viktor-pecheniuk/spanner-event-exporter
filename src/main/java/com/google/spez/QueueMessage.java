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

import com.google.auto.value.AutoValue;
import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;

/** Represents the unit of transmission and communication sent to and received by the Queue. */
@AutoValue
public abstract class QueueMessage {
  public abstract String queueName();

  public abstract String id();

  public abstract String key();

  public abstract ByteArray value();

  public abstract Timestamp timestamp();

  static QueueMessage create(
      String queueName, String id, String key, ByteArray value, Timestamp timestamp) {
    return new AutoValue_QueueMessage(queueName, id, key, value, timestamp);
  }
}
