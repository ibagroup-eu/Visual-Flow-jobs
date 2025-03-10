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
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.ElasticStageConfig
import by.iba.vf.spark.transformation.stage.ElasticStageConfig.index
import by.iba.vf.spark.transformation.stage.ElasticStageConfig.writeMode
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import by.iba.vf.spark.transformation.stage.WriteStageBuilder
import by.iba.vf.spark.transformation.utils.TruststoreGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[write] class ElasticWriteStage(
    override val configNode: Node,
    index: String,
    saveMode: Option[String],
    certDataPass: Option[(String, String, String)],
    elasticConfig: Map[String, String]
) extends WriteStage(configNode, ElasticStageConfig.storageId) {

  override val builder: StageBuilder = ElasticWriteStageBuilder

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {

    TruststoreGenerator.createTruststoreOnExecutors(
      spark,
      certDataPass
    )

    val dfWriter = getDfWriter(df, saveMode)
    dfWriter.format("org.elasticsearch.spark.sql").options(elasticConfig).save(index)
  }
}

object ElasticWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = ElasticStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean =
    ElasticStageConfig.validateElastic(config)

  override protected def convert(config: Node): Stage = {
    val ec = new ElasticStageConfig(config)
    val (jobMap, elasticMap, certDataPass) = ec.elasticParams
    new ElasticWriteStage(config, jobMap(index), jobMap.get(writeMode), certDataPass, getOptions(config.value) ++ elasticMap)
  }
}
