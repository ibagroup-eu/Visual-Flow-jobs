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
import by.iba.vf.spark.transformation.stage.read.JdbcReadStageBuilder.fieldStorage
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.security.SecureRandom

private[read] final class DataGeneratorStage(override val configNode: Node, alias: String)
  extends ReadStage(configNode, DataGeneratorStageBuilder.GeneratorStorage) {

  override val builder: StageBuilder = DataGeneratorStageBuilder

  override def read(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val bound = 100
    val data =
      Seq((1, 1), (2, 4), (3, new SecureRandom().nextInt(bound)))

    data.toDF("a", "b").as(alias)
  }
}

object DataGeneratorStageBuilder extends StageBuilder {
  private[read] val GeneratorStorage = "GENERATOR"
  private val FieldAlias = "alias"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.READ.toString) &&
      config.get(fieldStorage).contains(GeneratorStorage) && config.contains(FieldAlias)

  override protected def convert(config: Node): Stage = {
    val id = config.id
    val alias = config.value(FieldAlias)

    new DataGeneratorStage(config, alias)
  }
}
