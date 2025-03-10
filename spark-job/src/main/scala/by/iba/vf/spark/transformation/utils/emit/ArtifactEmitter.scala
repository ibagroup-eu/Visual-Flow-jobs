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
package by.iba.vf.spark.transformation.utils.emit

import by.iba.vf.spark.transformation.{Metadata, ResultLogger, RuntimeConfiguration, RuntimeConfigurationType, RuntimeModeType}
import by.iba.vf.spark.transformation.utils.MetadataUtils
import by.iba.vf.spark.transformation.utils.DatabricksUtils.putArtifactToUnityCatalog
import by.iba.vf.spark.transformation.utils.BackendRestUtils.{putJobDefinitionToRest, putStageMetadataToRest}

object ArtifactEmitter extends ResultLogger {

  def emitJobDefinition(definitionJsonString: String): Unit =
    RuntimeConfiguration.runtimeConfiguration match {
      case Some(RuntimeConfigurationType.DATABRICKS) => putArtifactToUnityCatalog("definition.json", definitionJsonString)
      case _ => putJobDefinitionToRest(definitionJsonString)
    }

  def emitStageMetadataArtifacts(meta: Metadata): Unit =
    RuntimeConfiguration.runtimeMode match {
      case Some(RuntimeModeType.INTERACTIVE) =>
        val stageMetadataJsonString = MetadataUtils.stringifyStageMeta(meta)
        RuntimeConfiguration.runtimeConfiguration match {
          case Some(RuntimeConfigurationType.DATABRICKS) => putArtifactToUnityCatalog(s"stages/metadata/${meta.id}.json", stageMetadataJsonString)
          case _ => putStageMetadataToRest(stageMetadataJsonString)
        }
      case _ => logger.info("No stage output artifact is emitted in the silent runtime mode")
    }

}
