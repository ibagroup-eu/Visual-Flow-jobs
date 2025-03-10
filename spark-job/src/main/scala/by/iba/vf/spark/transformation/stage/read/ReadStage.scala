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
import by.iba.vf.spark.transformation.stage.mixins.IncrementalLoad
import by.iba.vf.spark.transformation.stage.{OperationType, Stage}
import by.iba.vf.spark.transformation.Metadata
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

abstract class ReadStage(val configNode: Node, val storage: String) extends Stage with IncrementalLoad {
  override val operation: OperationType.Value = OperationType.READ
  override val inputsRequired: Int = 0
  private val incrementalLoadParams: Map[String, String] = getIncrementalLoadParams(configNode.value)

  def read(implicit spark: SparkSession): DataFrame

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    log(s"Reading from $storage, stage $id")
    Some(read)
      .map(applyIncrementalLoadBehavior(_, configNode.value))
  }

  override def deriveMetadata(df: DataFrame): Metadata = {
    val meta: Metadata = super.deriveMetadata(df)
    val offsetKey = incrementalLoadParams(fieldIncrementalOffsetKey)
    var offsetValue = incrementalLoadParams(fieldIncrementalOffsetValue)
    if (incrementalLoadParams(fieldIncrementalLoad).toBoolean) {
      val maxOffsetValueDataset = df.agg(max(offsetKey).alias(offsetKey)).collect()
      if (maxOffsetValueDataset.length > 0 && meta.count != 0)
        offsetValue = maxOffsetValueDataset(0).getAs[String](offsetKey)
      if (offsetValue != null)
        meta.nodeUpdate.value += fieldIncrementalOffsetValue -> offsetValue
    }

    meta
  }
}

