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
package by.iba.vf.spark.transformation.stage.config.objectstores

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import org.apache.spark.sql.SparkSession

class COSConfig(config: Node) extends BaseS3LikeStorageConfig(config) {
  final private val endpoint = config.value(fieldEndpoint)
  final private val iamApiKey: Option[String] = config.value.get(fieldIamApiKey)
  final private val iamServiceId: Option[String] = config.value.get(fieldIamServiceId)

  override def setConfig(spark: SparkSession): Unit = {
    spark.conf.set("fs.stocator.scheme.list", cosStorage)
    spark.conf.set(s"fs.$cosStorage.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.conf.set(s"fs.stocator.$cosStorage.impl", s"com.ibm.stocator.fs.$cosStorage.COSAPIClient")
    spark.conf.set(s"fs.stocator.$cosStorage.scheme", cosStorage)
    spark.conf.set(s"fs.$cosStorage.$service.endpoint", endpoint)
    authType match {
      case Some(auth) if authIAM.equals(auth) =>
        spark.conf.set(s"fs.$cosStorage.$service.iam.api.key", iamApiKey.orNull)
        spark.conf.set(s"fs.$cosStorage.$service.iam.service.id", iamServiceId.orNull)
      case Some(auth) if authHMAC.equals(auth) =>
        spark.conf.set(s"fs.$cosStorage.$service.access.key", accessKey.orNull)
        spark.conf.set(s"fs.$cosStorage.$service.secret.key", secretKey.orNull)
      case _ => throw new TransformationConfigurationException("Unknown authentication type for COS stage has been encountered")
    }
  }

  override def connectPath: String = s"$cosStorage://$bucket.$service/$path"
}

object COSConfig extends ObjectStorageS3LikeConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.contains(fieldEndpoint) &&
      config.get(fieldStorage).contains(cosStorage)
}
