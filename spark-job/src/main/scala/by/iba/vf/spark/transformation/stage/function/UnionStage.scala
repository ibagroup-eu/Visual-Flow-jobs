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
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[function] final class UnionStage(val id: String, unionAll: Boolean) extends Stage {
  override val operation: OperationType.Value = OperationType.UNION
  override val inputsRequired: Int = 2
  override val builder: StageBuilder = UnionStageBuilder

  @SuppressWarnings(Array("UnsafeTraversableMethods"))
  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    Some(input.values.reduceLeft(union))

  private def union(l: DataFrame, r: DataFrame): DataFrame =
    if (unionAll)
      l.union(r)
    else
      l.union(r).distinct()
}

object UnionStageBuilder extends StageBuilder {

  private val FieldType = "type"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.UNION.toString) && config.contains(FieldType)

  override protected def convert(config: Node): Stage = {
    val id = config.id
    val unionAll = config.value(FieldType) match {
      case "all"      => true
      case "distinct" => false
      case unionType  => throw new TransformationConfigurationException(s"Unknown union type: ${unionType}")
    }

    new UnionStage(id, unionAll)
  }
}
