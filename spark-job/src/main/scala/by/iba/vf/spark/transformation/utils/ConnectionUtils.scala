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

import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.config.formats.{Avro, CSV, Delta, JSON, ORC, Parquet, Text, WholeBinary}

object ConnectionUtils {

  def buildFormatOptions(config: Node, format: Option[String]): Map[String, String] = {
    val options: Map[String, String] = format.map {
      case "delta" => Delta.buildConnectionOptions(config)
      case "parquet" => Parquet.buildConnectionOptions(config)
      case "orc" => ORC.buildConnectionOptions(config)
      case "json" => JSON.buildConnectionOptions(config)
      case "csv" => CSV.buildConnectionOptions(config)
      case "text" => Text.buildConnectionOptions(config)
      case "avro" => Avro.buildConnectionOptions(config)
      case "binaryFile" => WholeBinary.buildConnectionOptions(config)
      case _ => throw new TransformationConfigurationException(s"Invalid value '${format.getOrElse("")}' of the parameter 'format'. " +
        "Allowed values: 'delta', 'parquet', 'orc', 'json', 'csv', 'text', 'avro'," +
        " 'binaryFile' (with exception of Databricks Native where binary is not supported).")
    }.getOrElse(Map[String, String]())

    options
  }

  def getPartitions(config: Node, fieldPartitionBy: String): Array[String] = {
    val partitionBy: Array[String] = config.value.get(fieldPartitionBy)
      .map(x => x.split(',').map(c => c.trim).filter(_.nonEmpty))
      .getOrElse(Array[String]())

    partitionBy
  }

}
