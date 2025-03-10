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

import by.iba.vf.spark.transformation.config.ProcessConfiguration
import by.iba.vf.spark.transformation.plan.{ExecutionPlan, ExecutionPlanBuilder}
import by.iba.vf.spark.transformation.utils.MetadataUtils
import by.iba.vf.spark.transformation.utils.emit.ArtifactEmitter
import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.{DataFrame, SparkSession}
import RuntimeConfiguration._
import ProcessConfiguration.{definitionParameterized, definitionParameterless, getDivergentStages, updatedConfigurationParameterizedUpdated, updatedConfiguration}
import by.iba.vf.spark.transformation.stage.Stage

object TransformationJob extends ResultLogger {

  private var ep: ExecutionPlan = ExecutionPlanBuilder.initEmptyExecutionPlan
  private var ed: ExecutionData = new ExecutionData(ep.dependencyMap)

  private def run(implicit spark: SparkSession): Unit = {
    ed.updateDependencies(ep.dependencyMap)
    if (runtimeMode.get != RuntimeModeType.INTERACTIVE)
      StagesState.stagesToRun = ep.dependencyMap.keys.toSet.union(ep.dependencyMap.values.flatten.toSet).toSeq
    ep.printExecutionPlan()
    ep.plan.foreach { iteration =>
      val results: Map[String, DataFrame] =
        iteration.flatMap { stage =>
          tryRunStage(stage)
        }.toMap
      ed.saveStageResults(results)
    }
  }

  private def tryRunStage(stage: Stage)(implicit spark: SparkSession): Option[(String, DataFrame)] =
    try {
      if (StagesState.stagesToRun.contains(stage.id))
        runStage(stage)
      else
        None
    } catch {
      case e: Exception =>
        runtimeMode match {
          case Some(RuntimeModeType.INTERACTIVE) => ArtifactEmitter.emitStageMetadataArtifacts(
            MetadataUtils.buildFailedMetadata(stage.id, e))
          logger.error(e.printStackTrace())
          case _ => throw e
        }
        ed.saveStageStatus(stage.id -> StageStatus.FAILED)
        None
    } finally {
      StagesState.stagesToRun.filterNot(_ == stage.id)
    }

  private def runStage(stage: Stage)(implicit spark: SparkSession): Option[(String, DataFrame)] = {
    logger.info("Current layer: {}", ed.getCurrentLayer)
    logger.info("Processing stage: {}", stage.id)
    logger.info("Stage input: {}", ep.dependencyMap.getOrElse(stage.id, Nil))
    // TODO: move such things like isUpstreamFailedAny & getDivergentStages to separate module like Execution Controller
    if (ed.isUpstreamFailedAny(ep.dependencyMap(stage.id))) {
      ed.saveStageStatus(stage.id -> StageStatus.UPSTREAM_FAILED)
      ArtifactEmitter.emitStageMetadataArtifacts(MetadataUtils.buildUpstreamFailedMetadata(stage.id))
      None
    } else {
      ArtifactEmitter.emitStageMetadataArtifacts(MetadataUtils.buildRunningMetadata(stage.id))
      stage.execute(ed.getStageInputData(stage.id)).map { r =>
        val meta = stage.deriveMetadata(r)
        ProcessConfiguration.applyNodeUpdate(definitionParameterized, meta.nodeUpdate)
        ArtifactEmitter.emitStageMetadataArtifacts(meta)
        ed.saveStageStatus(stage.id -> StageStatus.SUCCEEDED)
        stage.id -> r
      }
    }
  }

  private def emit(): Unit = {
    logger.info("Emitting updated job config")
    ArtifactEmitter.emitJobDefinition(ProcessConfiguration.decompose(definitionParameterized))
  }

  @SuppressWarnings(Array("CatchException"))
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().withExtensions(new CassandraSparkExtensions).getOrCreate()

    while (shouldContinue) {
      logger.info("Entering processing iteration")
      currentProcessingIteration += 1
      if (runtimeMode.get == RuntimeModeType.INTERACTIVE && currentProcessingIteration != 1) {
        logger.info(s"Sleeping $jobDefinitionPollIntervalInMilliseconds")
        Thread.sleep(jobDefinitionPollIntervalInMilliseconds)
      }
      if (anyStagesToRun || runtimeMode.get != RuntimeModeType.INTERACTIVE)
        iterate
    }

    // exit without stop Spark cluster for Databricks environment
    runtimeConfiguration match {
      case Some(RuntimeConfigurationType.KUBERNETES) => spark.stop()
      case _ =>
    }

  }

  private def anyStagesToRun: Boolean = {
    // TODO: Model Process Configuration, Execution Plan, Execution Data right to make it loosely coupled
    val (newDefinitionParameterized, newDefinition) = ProcessConfiguration.read
    updatedConfigurationParameterizedUpdated = newDefinitionParameterized
    updatedConfiguration = newDefinition
    ep = ExecutionPlanBuilder.buildExecutionPlan(updatedConfiguration)
    StagesState.stagesToRun = getDivergentStages(ep, ed)
    StagesState.stagesToRun.nonEmpty
  }

  private def iterate()(implicit spark: SparkSession): Unit =
    try {
      run
      emit()
    } catch {
      case e: Exception => logger.error(s"\nException in thread '${Thread.currentThread().getName}' $e\n\tat ${e.getStackTrace.mkString("\n\tat ")}")
        runtimeMode match {
          case Some(RuntimeModeType.INTERACTIVE) => logger.info("Continue running in the interactive mode")
          case _ =>
            spark.stop()
            System.exit(1)
        }
    }

}
