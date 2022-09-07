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

import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import scala.util.Using

abstract class ReadStage(val id: String, val storage: String) extends Stage {
  override val operation: OperationType.Value = OperationType.READ
  override val inputsRequired: Int = 0

  def read(implicit spark: SparkSession): DataFrame

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    log(s"Reading from $storage, stage $id")
    val data = Some(read)
    data.foreach(df => {
      Using(new ByteArrayOutputStream){ outCapture =>
        Console.withOut(outCapture) {
          df.show(10, truncate = false)
        }
        val result = new String(outCapture.toByteArray)
        log(f"Read data sample(top 10 rows):%n$result")
        log(s"Total number of rows read: ${df.count()}")
      }
    })
    data
  }
}

