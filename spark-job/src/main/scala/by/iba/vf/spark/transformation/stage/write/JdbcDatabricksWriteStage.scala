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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.{JdbcDatabricksStageBuilder, Stage, StageBuilder, WriteStageBuilder}

private[write] final class JdbcDatabricksWriteStage(
     configNode: Node,
     fullyQualifiedTable: String,
     truststorePath: Option[String],
     saveMode: Option[String],
     jdbcConfig: Map[String, String],
     truncateMode: TruncateMode.Value
) extends JdbcWriteStage(
    configNode: Node,
    fullyQualifiedTable: String,
    truststorePath: Option[String],
    saveMode: Option[String],
    jdbcConfig: Map[String, String],
    truncateMode: TruncateMode.Value
) {

  override val builder: StageBuilder = JdbcDatabricksWriteStageBuilder

}

object JdbcDatabricksWriteStageBuilder extends JdbcDatabricksStageBuilder with WriteStageBuilder {

  val fieldTruncateMode = "truncateMode"

  override def validateStorage(config: Map[String, String]): Boolean =
    config.get(fieldStorageId).exists(s => drivers.contains(s.toLowerCase))

  override protected def validateWrite(config: Map[String, String]): Boolean =
    validateJdbc(config)

  override protected def convert(config: Node): Stage = {
    val (fullyQualifiedTable, map) = jdbcParams(config)
    val truststorePathOption = if (config.value.contains(fieldCertData)) Some(truststorePath) else None
    val saveMode = config.value.get(fieldWriteMode)
    if (config.value.contains(fieldTruncateMode) && !TruncateMode.isKnownMode(config.value(fieldTruncateMode))) {
      throw new TransformationConfigurationException(s"Given truncate mode ${config.value(fieldTruncateMode)} is not supported")
    }
    val truncateMode = if (config.value.contains(fieldTruncateMode))
      TruncateMode.withName(config.value(fieldTruncateMode)) else TruncateMode.None
    new JdbcDatabricksWriteStage(config, fullyQualifiedTable, truststorePathOption, saveMode, map, truncateMode)
  }
}
