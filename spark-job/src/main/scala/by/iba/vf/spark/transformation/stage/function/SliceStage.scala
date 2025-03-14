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
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

private[function] final class SliceStage(val configNode: Node, mode: SliceType.Value, columns: Seq[String]) extends Stage {
  override val operation: OperationType.Value = OperationType.SLICE
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = SliceStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(slice)

  def slice(df: DataFrame): DataFrame = {
    mode match {
      case SliceType.DROP => drop(df)
      case SliceType.KEEP => keep(df)
    }
  }

  def drop(df: DataFrame): DataFrame =
    df.drop(columns:_*)

  def keep(df: DataFrame): DataFrame =
    df.select(columns.map(c => col(c)): _*)
}

object SliceStageBuilder extends StageBuilder {
  private val fieldMode = "mode"
  private val fieldColumns = "columns"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.SLICE.toString) &&
      config.contains(fieldMode) && config.contains(fieldColumns)

  override protected def convert(config: Node): Stage = {
    val columns: Seq[String] = config.value(fieldColumns).split(",\\s*").toSeq
    val mode: SliceType.Value = SliceType.withName(config.value(fieldMode).toUpperCase)
    new SliceStage(config, mode, columns)
  }
}

object SliceType extends Enumeration {
  val DROP, KEEP = Value
}
