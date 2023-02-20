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
import by.iba.vf.spark.transformation.stage.DataframeStageConfig.{dataFieldName, schemaFieldName, storageId}
import by.iba.vf.spark.transformation.stage.{DataframeStageConfig, ReadStageBuilder, Stage, StageBuilder}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Date, Timestamp}

class DataframeReadStage(override val id: String, config: DataframeStageConfig)
  extends ReadStage(id, DataframeStageConfig.storageId){

  override def read(implicit spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.makeRDD(typeCasting(config.parseData(), config.parseSchema()))
    val schema = createSparkSchema(config.parseSchema())
    spark.createDataFrame(rdd, schema)
  }

  def typeCasting(data: List[List[String]], schema: List[Map[String, String]]): List[Row] ={
    var castingData: List[Row] = List()
    for (rows <- data) {
      var castingRow: Seq[Any] = Seq()
      for (index <- rows.indices) {
        schema.apply(index).apply("type") match {
          case "Byte" => castingRow = castingRow :+ rows.apply(index).toByte
          case "Short" => castingRow = castingRow :+ rows.apply(index).toShort
          case "Integer" => castingRow = castingRow :+ rows.apply(index).toInt
          case "Long" => castingRow = castingRow :+ rows.apply(index).toLong
          case "Float" => castingRow = castingRow :+ rows.apply(index).toFloat
          case "Double" => castingRow = castingRow :+ rows.apply(index).toDouble
          case "String" => castingRow = castingRow :+ rows.apply(index)
          case "Boolean" => castingRow = castingRow :+ rows.apply(index).toBoolean
          case "Timestamp" => castingRow = castingRow :+ Timestamp.valueOf(rows.apply(index))
          case "Date" => castingRow = castingRow :+ Date.valueOf(rows.apply(index))
        }
      }
      castingData = castingData :+ Row.fromSeq(castingRow)
    }
    castingData
  }

  def createSparkSchema(schema: List[Map[String, String]]): StructType = {
    var fields: Array[StructField] = Array()
    for (columns <- schema) {
      columns.apply("type") match {
        case "Byte" => fields = fields :+ StructField(columns.apply("column"), ByteType)
        case "Short" => fields = fields :+ StructField(columns.apply("column"), ShortType)
        case "Integer" => fields = fields :+ StructField(columns.apply("column"), IntegerType)
        case "Long" => fields = fields :+ StructField(columns.apply("column"), LongType)
        case "Float" => fields = fields :+ StructField(columns.apply("column"), FloatType)
        case "Double" => fields = fields :+ StructField(columns.apply("column"), DoubleType)
        case "String" => fields = fields :+ StructField(columns.apply("column"), StringType)
        case "Boolean" => fields = fields :+ StructField(columns.apply("column"), BooleanType)
        case "Timestamp" => fields = fields :+ StructField(columns.apply("column"), TimestampType)
        case "Date" => fields = fields :+ StructField(columns.apply("column"), DateType)
      }
    }
    StructType(fields)
  }

  override val builder: StageBuilder = DataframeReadStageBuilder
}

object DataframeReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = DataframeStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    config.contains(dataFieldName) && config.contains(schemaFieldName)

  override protected def convert(config: Node): Stage = {
    new DataframeReadStage(config.id, new DataframeStageConfig(config))
  }
}
