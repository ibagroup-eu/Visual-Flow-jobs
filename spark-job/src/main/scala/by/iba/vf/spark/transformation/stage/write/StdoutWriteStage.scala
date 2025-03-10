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

import java.io.ByteArrayOutputStream

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Using

private[write] final class StdoutWriteStage(override val configNode: Node, quantity: Int)
  extends WriteStage(configNode, StdoutWriteStageBuilder.StdoutStorage) {

  override val builder: StageBuilder = StdoutWriteStageBuilder

  @SuppressWarnings(Array("UnsafeTraversableMethods"))
  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    val df = input.values.head
    log(s"Writing to stdout, stage $id")
    write(df)
    log(s"Total number of rows written: ${Math.min(quantity, df.count())}")
    Some(df)
  }

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    Using(new ByteArrayOutputStream){ outCapture =>
      Console.withOut(outCapture) {
        df.show(quantity, truncate = false)
      }
      val result = new String(outCapture.toByteArray)
      log(f"Written data (top $quantity rows):%n$result")
      log(f"Total row count: ${df.count()}")
    }
  }
}

object StdoutWriteStageBuilder extends StageBuilder {
  private[write] val StdoutStorage = "stdout"
  private val FieldStorage = "storage"
  val FieldQuantity = "quantity"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.WRITE.toString) &&
      StdoutStorage.equals(config.get(FieldStorage).orNull.toLowerCase) &&
      config.getOrElse(FieldQuantity, "1").forall(Character.isDigit)

  override protected def convert(config: Node): Stage = {
    val id = config.id
    new StdoutWriteStage(config, config.value.get(FieldQuantity).map(Integer.parseInt).getOrElse(10))
  }
}
