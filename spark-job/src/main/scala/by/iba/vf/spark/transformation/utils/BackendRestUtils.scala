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
package by.iba.vf.spark.transformation.utils

import by.iba.vf.spark.transformation.{ResultLogger, RuntimeConfiguration}
import requests.{RequestFailedException, Response}

object BackendRestUtils extends ResultLogger {

  private val host = RuntimeConfiguration.backendHost.get
  private val project = RuntimeConfiguration.projectId.get
  private val job = RuntimeConfiguration.jobId.get
  private val run = RuntimeConfiguration.runId.getOrElse("UNDEFINED")
  private val jobApi = s"$host/public/api/project/$project/job/$job"
  private val useSsl = false

  private def retryRestRequest(fn: => Response): Response = {
    val totalAttempts = 7
    val r = RequestUtils.retryRequest(totalAttempts)(fn)
    logger.info(s"Response: status code: ${r.statusCode}, status message: ${r.statusMessage}")
    r
  }

  private def putArtifactsToRest(fn: => Response): Unit =
    retryRestRequest(fn)

  private def getArtifactsFromRest(fn: => Response): Response =
    retryRestRequest(fn)

  def putJobDefinitionToRest(definitionJsonString: String): Unit =
    putArtifactsToRest(tryRequestJobDefinitionPatch(definitionJsonString))

  def getJobSessionFromRest: Option[String] =
    util.Try{ getArtifactsFromRest(tryRequestJobSessionGet) } match {
      case util.Success(r: Response) => Some(r.text())
      case util.Failure(e: RequestFailedException)
        if e.response.statusCode == 404 => None
      case util.Failure(e) => throw e
    }

  def getInteractiveJobEventsFromRest(queueOffset: Long): String =
    getArtifactsFromRest(tryRequestInteractiveJobEventsGet(queueOffset)).text()

  def putStageMetadataToRest(stageMetadataJsonString: String): Unit =
    putArtifactsToRest(tryRequestStageMetadataArtifacts(stageMetadataJsonString))

  private def tryRequestJobDefinitionPatch(definitionJsonString: String): Response = {
    logger.info(s"Emitting job definition: backend host $host, project $project, job $job.")
    logger.info(s"PATCH (ssl $useSsl): $jobApi/definition")
    logger.info(s"Data: $definitionJsonString")
    requests.patch(s"$jobApi/definition",
      headers = Map("Content-Type" -> "application/json"),
      data = definitionJsonString,
      verifySslCerts = useSsl)
  }

  private def tryRequestStageMetadataArtifacts(stageMetadataJsonString: String): Response = {
    logger.info(s"Emitting metadata artifacts: backend host $host, project $project, job $job, run $run.")
    logger.info(s"POST (ssl $useSsl): $jobApi/session/$run/metadata")
    logger.info(s"Data: $stageMetadataJsonString")
    requests.post(s"$jobApi/session/$run/metadata",
      headers = Map("Content-Type" -> "application/json"),
      data = stageMetadataJsonString,
      verifySslCerts = useSsl)
  }

  private def tryRequestJobSessionGet: Response = {
    logger.info(s"Obtaining job session: backend host $host, project $project, job $job, run $run.")
    logger.info(s"GET (ssl $useSsl): $jobApi/session/$run")
    val data = requests.get(s"$jobApi/session/$run",
      verifySslCerts = useSsl)
    logger.info(s"Response: $data")
    data
  }

  private def tryRequestInteractiveJobEventsGet(queueOffset: Long): Response = {
    val queryParams = Map(
      "offset" -> queueOffset
    )
    logger.info(s"Obtaining interactive job events: backend host $host, project $project, job $job, offset $queueOffset.")
    val params = queryParams.map { case (k, v) => s"$k=$v" }.mkString("&")
    logger.info(s"GET (ssl $useSsl): $jobApi/session/$run/events?$params")
    val data = requests.get(s"$jobApi/session/$run/events?$params",
      verifySslCerts = useSsl)
    logger.info(s"Response: $data")
    data
  }

}
