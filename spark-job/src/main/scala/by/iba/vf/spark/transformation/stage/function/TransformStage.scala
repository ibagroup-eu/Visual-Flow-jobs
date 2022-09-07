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
package by.iba.vf.spark.transformation.stage.function

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[function] final class TransformStage(val id: String, selectStmt: String, mode: TransformationMode.Value, tableName: Option[String]) extends Stage {
  override val operation: OperationType.Value = OperationType.TRANSFORM
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = TransformStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(transform(_, spark))

  def transform(df: DataFrame, spark: SparkSession): DataFrame = {
    val (tblName, query) = (mode, tableName) match {
      case (TransformationMode.Simple, _) => ("tmpTable", s"select $selectStmt from tmpTable")
      case (TransformationMode.Full_SQL, Some(name)) => (name, selectStmt)
      case (_, _) => throw new TransformationConfigurationException(s"Cannot process given transformation mode($mode) and table name($tableName)")
    }

    df.createOrReplaceTempView(tblName)
    spark.sql(query)
  }
}

object TransformStageBuilder extends StageBuilder {
  private val FieldStatement = "statement"
  private val FieldMode = "mode"
  private val FieldTableName = "tableName"

  override protected def validate(config: Map[String, String]): Boolean = {
    if (!(config.get(fieldOperation).contains(OperationType.TRANSFORM.toString) && config.contains(FieldStatement))) {
      return false
    }
    if (!config.contains(FieldMode)) {
      return true
    }
    TransformationMode.isKnownMode(config(FieldMode)) &&
      (
        (config(FieldMode) == TransformationMode.Simple.toString) ||
        (config(FieldMode) == TransformationMode.Full_SQL.toString && config.contains(FieldTableName))
      )
  }

  override protected def convert(config: Node): Stage =
    new TransformStage(
      config.id,
      config.value(FieldStatement),
      config.value.get(FieldMode).map(mode => TransformationMode.withName(mode)).getOrElse(TransformationMode.Simple),
      config.value.get(FieldTableName)
    )
}

object TransformationMode extends Enumeration {
  val Simple, Full_SQL = Value

  def isKnownMode(givenMode: String): Boolean = values.exists(value => value.toString == givenMode)
}
