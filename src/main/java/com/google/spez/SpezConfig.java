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

import com.typesafe.config.Config;

/**
 * Creates the wrapper by passing in a typesafe Config object.
 */
public class SpezConfig {
  public final int pollRate;
  public final String recordLimit;
  public final String projectId;
  public final String instanceName;
  public final String dbName;
  public final String tableName;
  public final String startingTimestamp;
  public final String avroNamespace;
  public final boolean publishToPubSub;

  /**
   * Wraps the typesafe config file.
   *
   * @param config the parsed typesafe Config object
   */
  public SpezConfig(Config config) {
    this.avroNamespace = config.getString("spez.avroNamespace");
    this.projectId = config.getString("spez.projectId");
    this.instanceName = config.getString("spez.instanceName");
    this.dbName = config.getString("spez.dbName");
    this.tableName = config.getString("spez.tableName");
    this.pollRate = config.getInt("spez.pollRate");
    this.recordLimit = config.getString("spez.recordLimit");
    this.startingTimestamp = config.getString("spez.startingTimestamp");
    this.publishToPubSub = config.getBoolean("spez.publishToPubSub");
  }
}
