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

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import org.apache.spark.sql.{DataFrameWriter, Row, SparkSession}

protected trait ObjectStorageConfig {
  final val cosStorage = "cos"
  final protected val objectStorage = "ObjectStorage"
  final val s3Storage = "s3"
  final protected val service = "service"

  final protected val fieldAccessKey = "accessKey"
  final protected val fieldBucket = "bucket"
  final protected val fieldEndpoint = "endpoint"
  final protected val fieldFormat = "format"
  final protected val fieldPath = "path"
  final protected val fieldWriteMode = "writeMode"
  final protected val fieldSecretKey = "secretKey"
  final protected val fieldIamApiKey = "iamApiKey"
  final protected val fieldIamServiceId = "iamServiceId"
  final protected val fieldStorage = "storage"
  final protected val fieldAnonymousAccess = "anonymousAccess"
  final protected val fieldAuthType = "authType"
  final protected val fieldSsl = "ssl"
  final protected val fieldUseSchema = "useSchema"
  final protected val partitionByField = "partitionBy"
  final protected val authHMAC = "HMAC"
  final protected val authIAM = "IAM"

  def validate(config: Map[String, String]): Boolean = {
    (
      config.contains(fieldAuthType) &&
        ((authHMAC.equals(config(fieldAuthType)) && config.contains(fieldAccessKey) && config.contains(fieldSecretKey)) ||
          (authIAM.equals(config(fieldAuthType)) && config.contains(fieldIamApiKey) && config.contains(fieldIamServiceId)))
        && config.contains(fieldBucket) && config.contains(fieldPath) && config.contains(fieldFormat)) ||
      (config.contains(fieldBucket) && config.contains(fieldPath) && config.contains(fieldFormat))
  }
}

protected abstract class BaseStorageConfig(config: Node) extends ObjectStorageConfig {
  final val format: String = config.value(fieldFormat)
  final val saveMode: Option[String] = config.value.get(fieldWriteMode)
  final val partitionBy: Option[Array[String]] = config.value.get(partitionByField).map(x => x.split(',').map(c => c.trim))
  final protected val id: String = config.id
  final protected val accessKey: Option[String] = config.value.get(fieldAccessKey)
  final protected val secretKey: Option[String] = config.value.get(fieldSecretKey)
  final protected val authType: Option[String] = config.value.get(fieldAuthType)
  final protected val bucket: String = config.value(fieldBucket)
  final protected val path: String = config.value(fieldPath)
  final val useSchema: Boolean = config.value.get(fieldUseSchema).exists(x => x.toBoolean)

  def setConfig(spark: SparkSession): Unit

  def connectPath: String

  def addPartitions(dfWriter: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    partitionBy.map(columns => dfWriter.partitionBy(columns: _*)).getOrElse(dfWriter)
  }
}

class COSConfig(config: Node) extends BaseStorageConfig(config) {
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

object COSConfig extends ObjectStorageConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.contains(fieldEndpoint) &&
      config.get(fieldStorage).contains(cosStorage)
}

class S3Config(config: Node) extends BaseStorageConfig(config) {
  final private val signed: Boolean = config.value(fieldAnonymousAccess).toBoolean
  final private val endpoint = config.value(fieldEndpoint)
  final private val ssl = config.value(fieldSsl)
  final private val pathStyle = (!config.value(fieldSsl).toBoolean).toString

  override def setConfig(spark: SparkSession): Unit = {
    if (signed) {
      spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    } else {
      spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      spark.conf.set("fs.s3a.access.key", accessKey.orNull)
      spark.conf.set("fs.s3a.secret.key", secretKey.orNull)
    }
    spark.conf.set("fs.s3a.endpoint", endpoint)
    spark.conf.set("fs.s3a.connection.ssl.enabled", ssl)
    spark.conf.set("fs.s3a.path.style.access", pathStyle)
  }

  override def connectPath: String = s"s3a://$bucket/$path"
}

object S3Config extends ObjectStorageConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.get(fieldStorage).contains(s3Storage) &&
      config.contains(fieldAnonymousAccess) &&
      config.contains(fieldEndpoint) &&
      config.contains(fieldSsl)
}
