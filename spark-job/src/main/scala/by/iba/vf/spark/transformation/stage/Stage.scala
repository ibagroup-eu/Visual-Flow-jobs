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
package by.iba.vf.spark.transformation.stage


import by.iba.vf.spark.transformation.ResultLogger
import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Stage extends ResultLogger {

  val id: String
  val operation: OperationType.Value
  val inputsRequired: Int

  val builder: StageBuilder

  def execute(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    logger.info("Executing stage \"{}\" (operation: {})", id: Any, operation: Any)
    process(input)
  }

  protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame]

}

trait StageBuilder {
  protected val fieldOperation = "operation"

  def validateAndConvert(config: Node): Option[Stage] =
    if (validateBase(config)) Some(convert(config)) else None

  private def validateBase(config: Node): Boolean =
    config.id.nonEmpty && config.value.nonEmpty && config.value.contains(fieldOperation) && validate(config.value)

  protected def validate(config: Map[String, String]): Boolean

  protected def convert(config: Node): Stage

  protected def getOptions(config: Map[String, String]): Map[String, String] = {
    val optionPattern = "option\\.(.+)".r
    config
      .flatMap {
        case (optionPattern(optionName), optionValue) => Some(optionName -> optionValue)
        case _ => None
      }
  }

}

trait StorageValidator {
  val fieldStorageId = "storage"

  protected def expectedStorage: String = ""

  protected def validateStorage(config: Map[String, String]): Boolean =
    config.get(fieldStorageId).exists(s => expectedStorage.equals(s))
}


trait ReadStageBuilder extends StageBuilder with StorageValidator {
  override protected def validate(config: Map[String, String]): Boolean = {
    config.get(fieldOperation).contains(OperationType.READ.toString) && validateRead(config) &&
      validateStorage(config)
  }

  protected def validateRead(config: Map[String, String]): Boolean
}

trait WriteStageBuilder extends StageBuilder with StorageValidator {
  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.WRITE.toString) && validateWrite(config) &&
      validateStorage(config)

  protected def validateWrite(config: Map[String, String]): Boolean
}

object OperationType extends Enumeration {
  val READ, WRITE, JOIN, UNION, GROUP, FILTER, TRANSFORM, SORT, CACHE, CDC, REMOVE_DUPLICATES = Value
}
