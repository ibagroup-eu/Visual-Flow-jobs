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
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage._
import by.iba.vf.spark.transformation.stage.config.objectstores.{AzureBlobStorageStageConfig, BaseStorageConfig, COSConfig, GoogleCloudStorageStageConfig, S3Config}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ObjectStorageWriteStage(
                     override val configNode: Node,
                     override val builder: StageBuilder,
                     config: BaseStorageConfig,
                     var options: Map[String, String],
                     cosStorage: String
                   ) extends WriteStage(configNode, cosStorage) {
  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    config.setConfig(spark)

    if (config.format.getOrElse("parquet") == "avro" && !config.useSchema)
      options = options.filter(elem => elem._1 != "avroSchema")

    val dfWriter = getDfWriter(df, config.saveMode)
      .options(options)

    config.addPartitions(dfWriter)
      .format(config.format.getOrElse("parquet"))
      .save(config.connectPath)
  }
}

object ObjectStorageWriteCOSStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = COSConfig.cosStorage

  override protected def validateWrite(config: Map[String, String]): Boolean =
    COSConfig.validate(config)

  override protected def convert(config: Node): Stage =
    new ObjectStorageWriteStage(config, this, new COSConfig(config), getOptions(config.value), COSConfig.cosStorage)
}

object ObjectStorageWriteS3StageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = S3Config.s3Storage

  override protected def validateWrite(config: Map[String, String]): Boolean =
    S3Config.validate(config)

  override protected def convert(config: Node): Stage = {
    val s3Config = new S3Config(config)
    new ObjectStorageWriteStage(config, this, s3Config, getOptions(config.value) ++ s3Config.options, S3Config.cosStorage)
  }
}

object AzureBlobStorageWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = AzureBlobStorageStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean =
    AzureBlobStorageStageConfig.validate(config)

  override protected def convert(config: Node): Stage = {
    val azureConfig = new AzureBlobStorageStageConfig(config)
    new ObjectStorageWriteStage(config, this, azureConfig, azureConfig.options ++ getOptions(config.value), AzureBlobStorageStageConfig.storageId)
  }
}

object GoogleCloudStorageWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = GoogleCloudStorageStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean =
    GoogleCloudStorageStageConfig.validate(config)

  override protected def convert(config: Node): Stage = {
    val gcpConfig = new GoogleCloudStorageStageConfig(config)
    new ObjectStorageWriteStage(config, this, gcpConfig, gcpConfig.options ++ getOptions(config.value), GoogleCloudStorageStageConfig.storageId)
  }
}
