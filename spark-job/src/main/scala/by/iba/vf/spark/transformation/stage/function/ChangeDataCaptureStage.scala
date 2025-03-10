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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import java.util.UUID.randomUUID

private object ChangeDataCaptureStage {
  private val OpNoChanges = 0
  private val OpInsert = 1
  private val OpDelete = 2
  private val OpUpdate = 3
}

private[function] class ChangeDataCaptureStage(
                                                val configNode: Node,
                                                keyColumns: Seq[String],
                                                newDataset: String,
                                                oldDataset: String,
                                                returnAll: Boolean
                                              ) extends Stage {
  override val operation: OperationType.Value = OperationType.CDC
  override val inputsRequired: Int = 2
  override val builder: StageBuilder = ChangeDataCaptureStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    Some(changeDataCapture(input(newDataset), input(oldDataset)))

  @SuppressWarnings(Array("CollectionPromotionToAny"))
  def changeDataCapture(newDF: DataFrame, oldDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val columns: Seq[Column] = keyColumns.map(new Column(_))

    val KEY_COLUMN_PLACEHOLDER = randomUUID.toString
    val NULL_VALUE_PLACEHOLDER = randomUUID.toString

    val oldDFOrdered = oldDF.orderBy(columns: _*).withColumn(KEY_COLUMN_PLACEHOLDER, columns.head)
    val newDFOrdered = newDF.orderBy(columns: _*).withColumn(KEY_COLUMN_PLACEHOLDER, columns.head)

    val newStartIdx = keyColumns.length
    val oldStartIdx = newDFOrdered.schema.length
    val optIndex = oldStartIdx + oldDFOrdered.schema.length - newStartIdx

    val joinedDF = newDFOrdered.join(oldDFOrdered, keyColumns, "outer").cache()
    val joinedSchema = joinedDF.schema

    val resultFields = {
      joinedSchema.slice(from = 0, newStartIdx) ++ joinedSchema.slice(newStartIdx, oldStartIdx) :+ StructField(
        "operation",
        IntegerType,
        nullable = true
      )
    }
    var initialRowsRdd = joinedDF.rdd
      .map { r =>
        val seq = r.toSeq
        val index = seq.slice(from = 0, newStartIdx)
        val newData = seq.slice(newStartIdx, oldStartIdx)
        val oldData = seq.slice(oldStartIdx, optIndex)
        (index, newData, oldData)
      }
    if (!returnAll) {
      initialRowsRdd = initialRowsRdd.filter { case (_, newData: Seq[Any], oldData: Seq[Any]) => newData != oldData }
    }
    val rowsRdd = initialRowsRdd
      .map {
        case (keyColumns: Seq[Any], _ :+ null, oldData: Seq[Any]) =>
          keyColumns ++ oldData :+ ChangeDataCaptureStage.OpDelete
        case (keyColumns: Seq[Any], newData: Seq[Any], _ :+ null) =>
          keyColumns ++ newData :+ ChangeDataCaptureStage.OpInsert
        case (keyColumns: Seq[Any], newData: Seq[Any], oldData: Seq[Any]) =>
          val newDataSortedSeq = newData.map {
            case null => NULL_VALUE_PLACEHOLDER
            case value => value.toString
          }.sorted
          val oldDataSortedSeq = oldData.map {
            case null => NULL_VALUE_PLACEHOLDER
            case value => value.toString
          }.sorted
          if (newDataSortedSeq != oldDataSortedSeq) {
            keyColumns ++ newData :+ ChangeDataCaptureStage.OpUpdate
          } else {
            keyColumns ++ oldData :+ ChangeDataCaptureStage.OpNoChanges
          }
      }
      .filter(_.nonEmpty)
      .map(Row.fromSeq)

    spark.createDataFrame(rowsRdd, StructType(resultFields)).drop(KEY_COLUMN_PLACEHOLDER)
  }
}

object ChangeDataCaptureStageBuilder extends StageBuilder {
  private val FieldKeyColumns = "keyColumns"
  private val FieldNewDataset = "newDataset"
  private val FieldOldDataset = "oldDataset"
  private val FieldMode = "mode"
  private val ModeDelta = "delta"
  private val ModeReturnAll = "all"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.CDC.toString) && config.contains(FieldKeyColumns) && config
      .contains(FieldNewDataset) && config.contains(FieldOldDataset) &&
      config.contains(FieldMode) && List(ModeDelta, ModeReturnAll).contains(config(FieldMode))

  override protected def convert(config: Node): Stage =
    new ChangeDataCaptureStage(
      config,
      config.value(FieldKeyColumns).split(",").map(_.trim),
      config.value(FieldNewDataset),
      config.value(FieldOldDataset),
      ModeReturnAll.equals(config.value(FieldMode))
    )
}
