/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.stage.config.objectstores

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.utils.ConnectionUtils
import org.apache.spark.sql.SparkSession

trait GoogleCloudStorageConfig extends ObjectStorageConfig {
  val fieldBucket = "bucket"
  val fieldPathToKeyFile = "pathToKeyFile"
}

object GoogleCloudStorageStageConfig extends GoogleCloudStorageConfig {
  val storageId = "google-cloud-storage"

  def validate(config: Map[String, String]): Boolean = {
      config.get(fieldStorage).contains(storageId) &&
      config.contains(fieldBucket) && config.contains(fieldPathToKeyFile)
  }

}

class GoogleCloudStorageStageConfig(config: Node) extends BaseStorageConfig(config) with GoogleCloudStorageConfig {
  private val bucket: String = config.value(fieldBucket)
  private val pathToKeyFile: String = config.value(fieldPathToKeyFile)
  val path: Option[String] = config.value.get(fieldPath)

  val options: Map[String, String] = ConnectionUtils.buildFormatOptions(config, format)

  override def connectPath: String =
    s"gs://$bucket/${path.getOrElse("")}"

  override def setConfig(spark: SparkSession): Unit =
    spark.conf.set("google.cloud.auth.service.account.json.keyfile", pathToKeyFile)

}
