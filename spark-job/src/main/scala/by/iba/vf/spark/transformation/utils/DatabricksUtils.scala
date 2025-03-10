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

import by.iba.vf.spark.transformation.RuntimeConfiguration
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object DatabricksUtils {

  private val job = RuntimeConfiguration.jobId.get
  private val secretScope = RuntimeConfiguration.databricksSecretScope.get
  private val artifactsPath = RuntimeConfiguration.databricksArtifactsPath.get
  private val jobArtifactsPath = s"$artifactsPath/$job"

  def getSecretValue(secretName: String): String =
    dbutils.secrets.get(secretScope, secretName)

  def putArtifactToUnityCatalog(path: String, artifact: String): Unit =
    dbutils.fs.put(s"$jobArtifactsPath/$path", artifact, overwrite = true)

  def getArtifactFromUnityCatalog(path: String): String =
    LocalFsUtils.readFile(s"$jobArtifactsPath/$path")

}
