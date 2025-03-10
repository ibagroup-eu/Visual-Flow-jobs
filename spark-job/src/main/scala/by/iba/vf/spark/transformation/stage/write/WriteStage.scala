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
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import scala.util.Using

protected abstract class WriteStage(val configNode: Node, storage: String) extends Stage {
  override val operation: OperationType.Value = OperationType.WRITE
  override val inputsRequired: Int = 1

  def write(df: DataFrame)(implicit spark: SparkSession): Unit

  @SuppressWarnings(Array("VariableShadowing"))
  protected def getDfWriter(df: DataFrame, saveMode: Option[String]): DataFrameWriter[Row] =
    saveMode.map(df.write.mode).getOrElse(df.write)

  @SuppressWarnings(Array("UnsafeTraversableMethods"))
  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    val df = input.values.head
    log(s"Writing to $storage, stage $id")
    write(df)
    logger.info("Write stage schema:\n{}", df.schema.treeString)
    logger.info("Total number of rows written: {}", df.count())
    Some(df)
  }
}

