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
package by.iba.vf.spark.transformation.config

import by.iba.vf.spark.transformation.{ExecutionData, ResultLogger, RuntimeConfiguration, RuntimeModeType, StageStatus}
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.plan.ExecutionPlan
import by.iba.vf.spark.transformation.utils.SecretsUtils
import by.iba.vf.spark.transformation.utils.emit.{InteractiveSession, RunCommand}
import net.liftweb.json.{DefaultFormats, compactRender, parse}
import net.liftweb.json.Extraction.{decompose => decomposeJson}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Base64

final case class ProcessConfiguration(var nodes: Seq[Node], edges: Seq[Edge])

object ProcessConfiguration extends ResultLogger {
  private implicit val Formats: DefaultFormats.type = DefaultFormats
  private val jobConfigPath = RuntimeConfiguration.jobDefinitionPath
  private val JobConfig: Option[String] = RuntimeConfiguration.jobDefinition
  private val runtimeMode: Option[RuntimeModeType.Value] = RuntimeConfiguration.runtimeMode
  private val paramPath = "$.text"

  // TODO: replace multiple versions of definition with something like Drifted Definition, Previous Definition
  // TODO: replace Process Configuration term with something reflecting reality like Jon Definition or Definition Service
  var (definitionParameterized, definitionParameterless): (ProcessConfiguration, ProcessConfiguration) = ProcessConfiguration.init
  var (updatedConfigurationParameterizedUpdated, updatedConfiguration): (ProcessConfiguration, ProcessConfiguration) = ProcessConfiguration.init

  private def init: (ProcessConfiguration, ProcessConfiguration) =
    (empty, empty)

  def empty: ProcessConfiguration =
    ProcessConfiguration(Seq(), Seq())

  def read: (ProcessConfiguration, ProcessConfiguration) = {
    val jobConfig: String = getJobConfig
    val configObjectParameterized = parse(jobConfig).extract[ProcessConfiguration]
    val configObject = parse(applyParams(jobConfig)).extract[ProcessConfiguration]
    (configObjectParameterized, configObject)
  }

  def readParameterless: ProcessConfiguration =
    parse(getJobConfig).extract[ProcessConfiguration]

  private def getJobConfig: String =
    runtimeMode match {
      case Some(RuntimeModeType.INTERACTIVE) => InteractiveSession.getJobDefinition
      case _ => jobConfigPath match {
        case Some(jobConfigPath) =>
          parseFile(jobConfigPath)
        case None =>
          decodeConfig
      }
    }

  def decompose(configObject: ProcessConfiguration): String =
    compactRender(decomposeJson(configObject))

  def applyNodeUpdate(config: ProcessConfiguration, update: Node): Unit = {
    config.nodes = config.nodes.map {
      case Node(`update`.id, m) => Node(`update`.id, m ++ update.value)
      case n => n
    }
  }

  def getDivergentStages(ep: ExecutionPlan, ed: ExecutionData): Seq[String] = {
    val drifted = ProcessConfiguration.getDivergentNodeIds(updatedConfiguration, definitionParameterless)
    logger.info(s"The following stages are drifted: ${drifted.mkString(", ")}")
    val divergent = runtimeMode match {
      case Some(RuntimeModeType.INTERACTIVE) =>
        var (commands, toRun) = InteractiveSession.getRunArtifacts
        logger.info(s"The following commands to run: ${commands.mkString(", ")}")
        logger.info(s"The following stages are obtained from events to run: ${toRun.mkString(", ")}")
        if (commands.contains(RunCommand.RUN_ALL.toString)) {
          ep.plan.flatten.map(_.id).toSet
        } else if (commands.contains(RunCommand.RUN_FAILED.toString)) {
          toRun = ed.stageStatus.collect {
            case (k, v) if v == StageStatus.FAILED => k
          }.toSet
          getDriftDependent(toRun, ep)
        } else {
          val runReferenced = getRunReferenced(toRun, ep)
          toRun ++ (runReferenced & getDriftDependent(drifted, ep))
        }
      case _ => drifted
    }
    if (divergent.isEmpty)
      logger.info("No divergent stages detected, nothing to run")
    else {
      logger.info(s"The following stages are divergent and set to run: ${divergent.mkString(", ")}")
      definitionParameterless = updatedConfiguration
      definitionParameterized = updatedConfigurationParameterizedUpdated
    }
    divergent.toSeq
  }

  private def getDriftDependent(driftedNodes: Set[String], ep: ExecutionPlan): Set[String] = {
    var dependent: Set[String] = driftedNodes
    ep.plan.foreach { iteration =>
      dependent ++= iteration.filter(stage => ep.dependencyMap(stage.id).exists(dependent.contains)).map(_.id)
    }
    logger.info(s"The following stages are drift dependent $dependent")
    dependent
  }

  private def getRunReferenced(runNodes: Set[String], ep: ExecutionPlan): Set[String] = {
    var referenced: Set[String] = runNodes
    def traverseUpstream(ids: Seq[String]): Unit =
      ids.foreach { id =>
        referenced += id
        traverseUpstream(ep.dependencyMap(id))
      }
    traverseUpstream(referenced.toSeq)
    logger.info(s"The following stages are referenced by the stages set to run $referenced")
    referenced
  }

  private def getNodesDrift(seq1: Seq[Node], seq2: Seq[Node]): Set[String] = {
    seq1.collect {
      case node if !seq2.contains(node) => node.id
    }.toSet
  }

  private def getEdgesDrift(seq1: Seq[Edge], seq2: Seq[Edge]): Set[String] = {
    seq1.collect {
      case edge if !seq2.contains(edge) => edge.target
    }.toSet
  }

  private def getDivergentNodeIds(current: ProcessConfiguration, previous: ProcessConfiguration): Set[String] =
    getNodesDrift(current.nodes, previous.nodes) union getEdgesDrift(current.edges, previous.edges)

  private def applyParams(str: String): String =
    "#([A-Za-z0-9\\-_]{1,50})#".r.replaceAllIn(str, matcher => getParam(matcher.group(1)))

  private def getParam(param: String): String = SecretsUtils.getParam(param)

  private def parseFile(filePathStr: String): String = {
    val filePath = Paths.get(filePathStr)
    val parsedConfig = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    logger.info(s"Parsed job config file $filePath, content $parsedConfig")
    parsedConfig
  }

  private def decodeConfig: String = {
    val jobConfigEncoded = JobConfig.getOrElse(throw new TransformationConfigurationException("Failed to get job configuration from configmaps"))
    new String(Base64.getDecoder.decode(jobConfigEncoded))
  }
}

final case class Edge(source: String, target: String)

final case class Node(id: String, var value: Map[String, String])
