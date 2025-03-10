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
import by.iba.vf.spark.transformation.{RuntimeConfiguration, RuntimeConfigurationType}

import java.util.Base64
import com.jayway.jsonpath.JsonPath

import java.nio.charset.StandardCharsets
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException

object SecretsUtils {

  private val paramPath = "$.text"

  def getParam(secretName: String): String = {
    val paramValue = JsonPath.read[String](getSecretValue(secretName), paramPath)
    RuntimeConfiguration.runtimeConfiguration match {
      case Some(RuntimeConfigurationType.DATABRICKS) => new String(Base64.getDecoder.decode(paramValue), StandardCharsets.UTF_8)
      case _ => paramValue
    }
  }

  private def getSecretValue(secretName: String): String =
    RuntimeConfiguration.runtimeConfiguration match {
      case Some(RuntimeConfigurationType.DATABRICKS) => DatabricksUtils.getSecretValue(secretName)
      case _ => sys.env.getOrElse(
        secretName,
        throw new TransformationConfigurationException(s"Failed to acquire param: $secretName")
      )
    }

}
