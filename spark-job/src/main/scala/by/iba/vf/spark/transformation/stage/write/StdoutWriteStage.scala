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
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[write] final class StdoutWriteStage(override val id: String, quantity: Int)
  extends WriteStage(id, StdoutWriteStageBuilder.StdoutStorage) {

  override val builder: StageBuilder = StdoutWriteStageBuilder

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    df.show(quantity, truncate = false)
    printf("Total row count: %d%n", df.count())
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
    new StdoutWriteStage(id, config.value.get(FieldQuantity).map(Integer.parseInt).getOrElse(10))
  }
}
