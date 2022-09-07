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
import by.iba.vf.spark.transformation.stage.COSConfig
import by.iba.vf.spark.transformation.stage.BaseStorageConfig
import by.iba.vf.spark.transformation.stage.ReadStageBuilder
import by.iba.vf.spark.transformation.stage.S3Config
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class ObjectStorageReadStage(
                    override val id: String,
                    override val builder: StageBuilder,
                    conf: BaseStorageConfig,
                    var options: Map[String, String],
                    cosStorage: String
) extends ReadStage (id, cosStorage) {
  override def read(implicit spark: SparkSession): DataFrame = {
    conf.setConfig(spark)

    if (conf.format == "avro" && !conf.useSchema)
      options = options.filter(elem => elem._1 != "avroSchema")

    spark.read
      .options(options)
      .format(conf.format)
      .load(conf.connectPath)
  }
}

object ObjectStorageReadCOSStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = COSConfig.cosStorage

  override protected def validateRead(config: Map[String, String]): Boolean =
    COSConfig.validate(config)

  override protected def convert(config: Node): Stage =
    new ObjectStorageReadStage(config.id, this, new COSConfig(config), getOptions(config.value), COSConfig.cosStorage)
}

object ObjectStorageReadS3StageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = S3Config.s3Storage

  override protected def validateRead(config: Map[String, String]): Boolean =
    S3Config.validate(config)

  override protected def convert(config: Node): Stage =
    new ObjectStorageReadStage(config.id, this, new S3Config(config), getOptions(config.value), S3Config.cosStorage)
}
