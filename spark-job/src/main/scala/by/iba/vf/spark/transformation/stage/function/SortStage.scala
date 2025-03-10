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
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, asc_nulls_first, asc_nulls_last}
import org.apache.spark.sql.functions.{desc, desc_nulls_first, desc_nulls_last}

private[function] final class SortStage(val configNode: Node, sortType: String, orderColumns: Seq[Column]) extends Stage {
  override val operation: OperationType.Value = OperationType.SORT
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = SortStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(sort)

  def sort(df: DataFrame): DataFrame = {
    sortType match {
      case "fullSort" => df.orderBy(orderColumns: _*)
      case "sortWithinPartitions" => df.sortWithinPartitions(orderColumns: _*)
      case _ =>
        throw new TransformationConfigurationException(s"Invalid sort type: $sortType")
    }
  }
}

object SortStageBuilder extends StageBuilder {
  private val FieldSortType = "sortType"
  private val FieldOrderColumns = "orderColumns"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.SORT.toString) &&
      config.contains(FieldSortType) &&
      config.contains(FieldOrderColumns)

  override protected def convert(config: Node): Stage = {
    val comma = ","
    val orderColumns = config.value(FieldOrderColumns).split(comma).map { colStr =>
      colStr.trim.split(":").toList match {
        case colName :: "desc" :: Nil => desc(colName)
        case colName :: "desc_nulls_first" :: Nil => desc_nulls_first(colName)
        case colName :: "desc_nulls_last" :: Nil => desc_nulls_last(colName)
        case colName :: "asc" :: Nil  => asc(colName)
        case colName :: "asc_nulls_first" :: Nil => asc_nulls_first(colName)
        case colName :: "asc_nulls_last" :: Nil => asc_nulls_last(colName)
        case colName :: Nil           => asc(colName)
        case _ =>
          throw new TransformationConfigurationException(s"Bad order column format: $colStr")
      }
    }
    val sortType = config.value(FieldSortType)

    new SortStage(config, sortType, orderColumns.toSeq)
  }
}
