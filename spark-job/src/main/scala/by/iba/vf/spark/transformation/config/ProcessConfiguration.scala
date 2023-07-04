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

import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import net.liftweb.json
import com.jayway.jsonpath.JsonPath
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

final case class ProcessConfiguration(nodes: Seq[Node], edges: Seq[Edge])

object ProcessConfiguration {
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats
  private val JobConfig: String = "JOB_CONFIG"
  private val JobConfigPath: String = "JOB_CONFIG_PATH"
  private val paramPath = "$.text"

  def read: ProcessConfiguration = {
    val jobConfigPath = sys.env.get(JobConfigPath)
    val jobConfig: String = {
      jobConfigPath match {
        case Some(jobConfigPath) =>
          parseFile(jobConfigPath)
        case None =>
          sys.env.getOrElse(JobConfig, throw new TransformationConfigurationException("Failed to get job configuration from configmaps"))
      }
    }
    json.parse(applyParams(jobConfig)).extract[ProcessConfiguration]
  }

  private def applyParams(str: String): String =
    "#([A-Za-z0-9\\-_]{1,50})#".r.replaceAllIn(str, matcher => JsonPath.read[String](getParam(matcher.group(1)), paramPath))

  private def getParam(param: String): String = sys.env.getOrElse(
    param,
    throw new TransformationConfigurationException(s"Failed to acquire param: $param")
  )

  private def parseFile(filePathStr: String): String = {
    val filePath = Paths.get(filePathStr)
    new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
  }
}

final case class Edge(source: String, target: String)

final case class Node(id: String, value: Map[String, String])
