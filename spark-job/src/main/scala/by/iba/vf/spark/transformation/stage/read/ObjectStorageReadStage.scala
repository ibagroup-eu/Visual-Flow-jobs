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
package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.ReadStageBuilder
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import by.iba.vf.spark.transformation.stage.config.objectstores.{AzureBlobStorageStageConfig, BaseStorageConfig, COSConfig, GoogleCloudStorageStageConfig, S3Config}
import by.iba.vf.spark.transformation.utils.BinaryUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, max}

class ObjectStorageReadStage(
                    override val configNode: Node,
                    override val builder: StageBuilder,
                    conf: BaseStorageConfig,
                    var options: Map[String, String],
                    cosStorage: String
) extends ReadStage (configNode, cosStorage) {
  override def read(implicit spark: SparkSession): DataFrame = {
    conf.setConfig(spark)

    val format = conf.format.getOrElse("parquet")

    if (format == "avro" && !conf.useSchema)
      options = options.filter(elem => elem._1 != "avroSchema")

    var df = spark.read
      .options(options)
      .format(conf.format.getOrElse("parquet"))
      .load(conf.connectPath)

    if (format == "binaryFile") {
      if (List("pdf", "doc", "docx").contains(options("binaryFormat")))
        df = BinaryUtils.process(spark, options("binaryFormat"), df)

      df = df
        .withColumnsRenamed(Map(
          "path" -> options("outputPathColumn"),
          "content" -> options("outputContentColumn")
        ))
        .select(options("outputPathColumn"), options("outputContentColumn"))
    }

    df
  }

}

object ObjectStorageReadCOSStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = COSConfig.cosStorage

  override protected def validateRead(config: Map[String, String]): Boolean =
    COSConfig.validate(config)

  override protected def convert(config: Node): Stage =
    new ObjectStorageReadStage(config, this, new COSConfig(config), getOptions(config.value), COSConfig.cosStorage)
}

object ObjectStorageReadS3StageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = S3Config.s3Storage

  override protected def validateRead(config: Map[String, String]): Boolean =
    S3Config.validate(config)

  override protected def convert(config: Node): Stage = {
    val s3Config = new S3Config(config)
    new ObjectStorageReadStage(config, this, s3Config, getOptions(config.value) ++ s3Config.options, S3Config.cosStorage)
  }
}

object AzureBlobStorageReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = AzureBlobStorageStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    AzureBlobStorageStageConfig.validate(config)

  override protected def convert(config: Node): Stage = {
    val azureConfig = new AzureBlobStorageStageConfig(config)
    new ObjectStorageReadStage(config, this, azureConfig, azureConfig.options ++ getOptions(config.value), AzureBlobStorageStageConfig.storageId)
  }
}

object GoogleCloudStorageReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = GoogleCloudStorageStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    GoogleCloudStorageStageConfig.validate(config)

  override protected def convert(config: Node): Stage = {
    val gcpConfig = new GoogleCloudStorageStageConfig(config)
    new ObjectStorageReadStage(config, this, gcpConfig, gcpConfig.options ++ getOptions(config.value), GoogleCloudStorageStageConfig.storageId)
  }
}
