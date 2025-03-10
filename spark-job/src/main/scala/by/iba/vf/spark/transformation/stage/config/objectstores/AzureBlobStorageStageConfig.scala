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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.utils.ConnectionUtils
import org.apache.spark.sql.SparkSession

trait AzureBlobStorageConfig extends ObjectStorageConfig {
  val fieldContainer = "container"
  val fieldContainerPath = "containerPath"
  val fieldStorageAccount = "storageAccount"
  val fieldAuthType = "authType"
  val authStorageAccountKey = "storageAccountKey"
  val authSASToken = "SASToken"
  val fieldStorageAccountKey = "storageAccountKey"
  val fieldSASToken = "SASToken"
}

object AzureBlobStorageStageConfig extends AzureBlobStorageConfig {
  val storageId = "azure-blob-storage"

  def validate(config: Map[String, String]): Boolean = {
    (
      config.get(fieldStorage).contains(storageId) &&
      config.contains(fieldAuthType) &&
        ((authStorageAccountKey.equals(config(fieldAuthType)) && config.contains(fieldStorageAccountKey)) ||
          (authSASToken.equals(config(fieldAuthType)) && config.contains(fieldSASToken)))
        && config.contains(fieldContainer) && config.contains(fieldStorageAccount)
        && config.contains(fieldContainerPath) && config.contains(fieldFormat))
  }

}

class AzureBlobStorageStageConfig(config: Node) extends BaseStorageConfig(config) with AzureBlobStorageConfig {
  private val container: String = config.value(fieldContainer)
  private val storageAccount: String = config.value(fieldStorageAccount)
  val authType: Option[String] = config.value.get(fieldAuthType)
  private val storageAccountKey: Option[String] = config.value.get(fieldStorageAccountKey)
  private val SASToken: Option[String] = config.value.get(fieldSASToken)
  val path: Option[String] = config.value.get(fieldContainerPath)

  val options: Map[String, String] = ConnectionUtils.buildFormatOptions(config, format)

  override def connectPath: String =
    s"wasbs://$container@$storageAccount.blob.core.windows.net/${path.getOrElse("")}"

  override def setConfig(spark: SparkSession): Unit =
    authType match {
      case Some(auth) if authStorageAccountKey.equals(auth) =>
        spark.conf.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", storageAccountKey.getOrElse(""))
      case Some(auth) if authSASToken.equals(auth) =>
        spark.conf.set(s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net", SASToken.getOrElse(""))
      case _ => throw new TransformationConfigurationException("Unknown authentication type for Azure Blob Storage stage has been encountered")
    }

}
