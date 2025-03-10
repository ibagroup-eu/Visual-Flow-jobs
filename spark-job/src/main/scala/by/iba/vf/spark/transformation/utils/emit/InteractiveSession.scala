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

import by.iba.vf.spark.transformation.{ResultLogger, RuntimeConfiguration, RuntimeConfigurationType, RuntimeModeType}
import by.iba.vf.spark.transformation.utils.BackendRestUtils
import by.iba.vf.spark.transformation.utils.BackendRestUtils.getInteractiveJobEventsFromRest
import by.iba.vf.spark.transformation.utils.DatabricksUtils.getArtifactFromUnityCatalog
import net.liftweb.json.{DefaultFormats, parse}

case class Event(session: String, command: String, id: Seq[String])
case class InteractiveJobEvents(offset: Long, data: Seq[Event])

object SessionStatus extends Enumeration {
  val ACTIVE, INACTIVE = Value
}

object RunCommand extends Enumeration {
  val RUN: RunCommand.Value = Value("run")
  val RUN_FAILED: RunCommand.Value = Value("run-failed")
  val RUN_ALL: RunCommand.Value = Value("run-all")
}

object InteractiveSession extends ResultLogger {
  private implicit val Formats: DefaultFormats.type = DefaultFormats
  private var eventQueueOffset: Long = 0
  private var status = SessionStatus.INACTIVE
  private var jobDefinition: String = "{}"

  def getJobSessionStatus: SessionStatus.Value = {
    jobDefinition = getDefinitionPayload match {
      case Some(sessionJson) =>
        status = SessionStatus.ACTIVE
        sessionJson
      case _ =>
        status = SessionStatus.INACTIVE
        jobDefinition
    }
    logger.info(s"Job session status is obtained: $status")
    status
  }

  private def getDefinitionPayload: Option[String] =
    RuntimeConfiguration.runtimeConfiguration match {
      case Some(RuntimeConfigurationType.DATABRICKS) => Some(getArtifactFromUnityCatalog("interactive/session.json"))
      case _ => RuntimeConfiguration.runtimeMode match {
        case Some(RuntimeModeType.INTERACTIVE) => BackendRestUtils.getJobSessionFromRest
        case _ => None
      }
    }

  def getJobDefinition: String =
    jobDefinition

  def getRunArtifacts: (Set[String], Set[String]) = {
    val eventsJson = getInteractiveJobEventsFromRest(eventQueueOffset)
    val interactiveJobEvents = parse(eventsJson).extractOrElse[InteractiveJobEvents](InteractiveJobEvents(0, Seq()))
    eventQueueOffset = interactiveJobEvents.offset
    val commands: Set[String] = interactiveJobEvents.data.collect {
      case event => event.command
    }.toSet
    val ids: Set[String] = interactiveJobEvents.data.collect {
      case event => event.id
    }.flatten.toSet
    (commands, ids)
  }

}
