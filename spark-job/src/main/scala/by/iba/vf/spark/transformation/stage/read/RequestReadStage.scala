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
package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{ReadStageBuilder, RequestStageConfig, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RequestReadStage(override val id: String, config: RequestStageConfig, options: Map[String, String])
  extends ReadStage(id, RequestStageConfig.storageId){


  override def read(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val optionParams = options.get("params").mkString
    val optionHeaders = options.get("headers").mkString
    val request = requests.send(config.method)(
      config.host,
      params = if (optionParams.isEmpty) Map.empty else config.makeMap(optionParams),
      headers = if (optionHeaders.isEmpty) Map.empty else config.makeMap(optionHeaders)
    )
    spark.read.option("multiline","true").json(Seq(request.text()).toDS())
  }




  override val builder: StageBuilder = RequestReadStageBuilder
}

object RequestReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = RequestStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean = {
    config.contains(RequestStageConfig.hostFieldName) && config.contains(RequestStageConfig.methodFieldName)
  }

  override protected def convert(config: Node): Stage =
    new RequestReadStage(config.id, new RequestStageConfig(config), getOptions(config.value))
}

