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
package by.iba.vf.spark.transformation.utils

import by.iba.vf.spark.transformation.{Metadata, StageStatus}
import by.iba.vf.spark.transformation.config.Node
import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

final case class SchemaField(field: String, `type`: String)
final case class Exemplar(records: Array[Map[String, Any]])
final case class MetadataArtifacts(id: String, status: String, statusMessage: String, rowCount: Long, schema: Seq[SchemaField], exemplar: Exemplar)

object MetadataUtils {
  implicit val formats: Formats = DefaultFormats

  def stringifyStageMeta(meta: Metadata): String = {
    val schema = meta.schema.fields.map(f =>
      SchemaField(f.name, f.dataType.catalogString))
    val fieldsList = schema.map(_.field)

    val metadataArtifacts = MetadataArtifacts(
      meta.id,
      meta.status.toString.toLowerCase,
      meta.statusMessage,
      meta.count,
      schema,
      Exemplar(
        meta.exemplar.rdd.collect()
          .map(_.getValuesMap(fieldsList))
      ))

    compact(render(Extraction.decompose(metadataArtifacts)))
  }

  def buildFailedMetadata(id: String, e: Exception)(implicit spark: SparkSession): Metadata = {
    val emptySchema = StructType(Seq())
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)
    Metadata(id, StageStatus.FAILED, e.getStackTrace.mkString("\n\tat "), 0, emptySchema, emptyDF, Node(id, Map()))
  }

  def buildUpstreamFailedMetadata(id: String)(implicit spark: SparkSession): Metadata = {
    val emptySchema = StructType(Seq())
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)
    Metadata(id, StageStatus.UPSTREAM_FAILED, "Stage didn't run since one of the upstream stages has failed", 0, emptySchema, emptyDF, Node(id, Map()))
  }

  def buildRunningMetadata(id: String)(implicit spark: SparkSession): Metadata = {
    val emptySchema = StructType(Seq())
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)
    Metadata(id, StageStatus.RUNNING, "Stage is running", 0, emptySchema, emptyDF, Node(id, Map()))
  }

}
