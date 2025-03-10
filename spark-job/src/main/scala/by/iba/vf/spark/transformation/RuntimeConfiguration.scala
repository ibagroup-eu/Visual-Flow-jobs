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
package by.iba.vf.spark.transformation

import by.iba.vf.spark.transformation.utils.emit.SessionStatus
import by.iba.vf.spark.transformation.utils.emit.InteractiveSession.getJobSessionStatus

class EnumerationWithNameOpt extends Enumeration {
  def withNameOpt(name: String): Option[Value] = values.find(_.toString.equalsIgnoreCase(name))
}

object RuntimeConfigurationType extends EnumerationWithNameOpt {
  val DATABRICKS: RuntimeConfigurationType.Value = Value("Databricks")
  val KUBERNETES: RuntimeConfigurationType.Value = Value("Kubernetes")
}

object RuntimeModeType extends EnumerationWithNameOpt {
  val SILENT: RuntimeModeType.Value = Value("SILENT")
  val INTERACTIVE: RuntimeModeType.Value = Value("INTERACTIVE")
}

object RuntimeConfiguration extends ResultLogger {

  logger.info("Set runtime configuration with environment variables")

  logEnvVar("VISUAL_FLOW_CONFIGURATION_TYPE")
  val runtimeConfiguration: Option[RuntimeConfigurationType.Value] = RuntimeConfigurationType.withNameOpt(
    sys.env.getOrElse("VISUAL_FLOW_CONFIGURATION_TYPE", RuntimeConfigurationType.KUBERNETES.toString))
  logEnvVar("VISUAL_FLOW_RUNTIME_MODE")
  val runtimeMode: Option[RuntimeModeType.Value] = RuntimeModeType.withNameOpt(
    sys.env.getOrElse("VISUAL_FLOW_RUNTIME_MODE", RuntimeModeType.SILENT.toString))
  val jobId: Option[String] = initEnvVar("JOB_ID")
  val projectId: Option[String] = initEnvVar("POD_NAMESPACE")
  val runId: Option[String] = initEnvVar("POD_UID")
  val backendHost: Option[String] = initEnvVar("BACKEND_HOST")
  val databricksSecretScope: Option[String] = initEnvVar("VISUAL_FLOW_DATABRICKS_SECRET_SCOPE")
  val databricksArtifactsPath: Option[String] = initEnvVar("VISUAL_FLOW_DATABRICKS_ARTIFACTS_PATH")
  val jobDefinitionPath: Option[String] = initEnvVar("JOB_CONFIG_PATH")
  val jobDefinition: Option[String] = initEnvVar("JOB_CONFIG")
  val jobDefinitionPollIntervalInMilliseconds: Long = initEnvVar("JOB_DEFINITION_POLL_INTERVAL_IN_MILLISECONDS")
    .flatMap(value => scala.util.Try(value.toLong).toOption)
    .getOrElse(1000)
  private val totalProcessingIterationsAllowed: Long = runtimeMode match {
    case Some(RuntimeModeType.INTERACTIVE) => initEnvVar("INTERACTIVE_MODE_TOTAL_PROCESSING_ITERATIONS_ALLOWED")
      .flatMap(value => scala.util.Try(value.toLong).toOption)
      .getOrElse(24 * 3600)
    case _ => 1
  }
  var currentProcessingIteration: Long = 0
  private val clusterTotalRunTimeInMillisecondsAllowed: Long = runtimeMode match {
    case Some(RuntimeModeType.INTERACTIVE) => initEnvVar("INTERACTIVE_MODE_CLUSTER_TOTAL_RUNTIME_IN_MILLISECONDS_ALLOWED")
      .flatMap(value => scala.util.Try(value.toLong).toOption)
      .getOrElse(24 * 3600 * 1000)
    case _ => 0
  }
  private val clusterStartTimeInMilliseconds: Long = System.currentTimeMillis()

  private def isSessionActive: Boolean =
    runtimeMode match {
      case Some(RuntimeModeType.SILENT) => true
      case _ => getJobSessionStatus == SessionStatus.ACTIVE
    }

  def shouldContinue: Boolean = {
    logger.info("Checking whether exit processing iteration condition is met")
    logger.info(s"Custer start time in milliseconds: $clusterStartTimeInMilliseconds")
    logger.info(s"Run time watermark: ${clusterStartTimeInMilliseconds + clusterTotalRunTimeInMillisecondsAllowed}")
    logger.info(s"Current time: ${System.currentTimeMillis()}")
    isInteractiveSessionAlive || isSilentRunSingleIteration
  }

  private def isInteractiveSessionAlive: Boolean =
    runtimeMode.get == RuntimeModeType.INTERACTIVE && isSessionActive &&
      currentProcessingIteration < totalProcessingIterationsAllowed &&
      System.currentTimeMillis() < clusterStartTimeInMilliseconds + clusterTotalRunTimeInMillisecondsAllowed

  private def isSilentRunSingleIteration: Boolean =
    runtimeMode.get == RuntimeModeType.SILENT && currentProcessingIteration < totalProcessingIterationsAllowed

  private def initEnvVar(name: String): Option[String] = {
    logEnvVar(name)
    sys.env.get(name)
  }

  private def logEnvVar(name: String): Unit =
    logger.info(s"Environment variable: name=$name, value=${sys.env.getOrElse(name, "Undefined")}")
}
