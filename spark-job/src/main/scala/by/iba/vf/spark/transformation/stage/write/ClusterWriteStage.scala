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
import by.iba.vf.spark.transformation.stage.ClusterStageConfig.{formatFieldName, pathFieldName}
import by.iba.vf.spark.transformation.stage.{ClusterStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.zeroturnaround.zip.ZipUtil
import java.io.File

class ClusterWriteStage(override val configNode: Node, var options: Map[String, String], config: ClusterStageConfig)
  extends WriteStage(configNode, ClusterStageConfig.storageId) {

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    if (config.format == "avro" && !config.useSchema)
      options = options.filter(elem => elem._1 != "avroSchema")

    addPartitions(getDfWriter(df, Option("overwrite")).options(options)).format(config.format).save(s"/files/${config.path}")
    ZipUtil.pack(new File(s"/files/${config.path}"), new File(s"/files/${config.path}.zip"));
  }

  def addPartitions(dfWriter: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    config.partitionBy.map(columns => dfWriter.partitionBy(columns: _*)).getOrElse(dfWriter)
  }

  override val builder: StageBuilder = ClusterWriteStageBuilder
}

object ClusterWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = ClusterStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    config.contains(pathFieldName) && config.contains(formatFieldName)
  }

  override protected def convert(config: Node): Stage =
    new ClusterWriteStage(config,  getOptions(config.value), new ClusterStageConfig(config))
}
