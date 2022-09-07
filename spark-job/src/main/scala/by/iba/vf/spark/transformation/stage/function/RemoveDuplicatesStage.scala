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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.row_number

class RemoveDuplicatesStage(val id: String, keyColumns: Seq[Column], orderColumns: Seq[Column]) extends Stage {
  override val operation: OperationType.Value = OperationType.REMOVE_DUPLICATES
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = RemoveDuplicatesStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(removeDuplicates)

  def removeDuplicates(df: DataFrame): DataFrame = {
    val rowNumberColumn = "row_number"
    df.withColumn(
      rowNumberColumn,
      row_number()
        .over(
          Window
            .partitionBy(keyColumns: _*)
            .orderBy(orderColumns: _*)
        )
    ).filter(col(rowNumberColumn) === 1)
      .drop(rowNumberColumn)
  }
}

object RemoveDuplicatesStageBuilder extends StageBuilder {
  private val FieldKeyColumns = "keyColumns"
  private val FieldOrderColumns = "orderColumns"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.REMOVE_DUPLICATES.toString) &&
      config.contains(FieldKeyColumns) &&
      config.contains(FieldOrderColumns)

  override protected def convert(config: Node): Stage = {
    val comma = ","
    val keyColumns = config.value(FieldKeyColumns).split(comma).map(col)
    val orderColumns = config.value(FieldOrderColumns).split(comma).map { colStr =>
      colStr.trim.split(":").toList match {
        case colName :: "desc" :: Nil => desc(colName)
        case colName :: "asc" :: Nil  => asc(colName)
        case colName :: Nil           => asc(colName)
        case _ =>
          throw new TransformationConfigurationException(s"bad order column format: $colStr")
      }
    }
    new RemoveDuplicatesStage(config.id, keyColumns.toSeq, orderColumns.toSeq)
  }
}
