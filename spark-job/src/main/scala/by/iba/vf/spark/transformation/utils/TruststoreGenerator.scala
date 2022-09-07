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

import java.io.ByteArrayInputStream
import java.io.OutputStream
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.Base64

import by.iba.vf.spark.transformation.ResultLogger
import org.apache.spark.sql.SparkSession

/**
 * Utils related to truststore generation
 */
object TruststoreGenerator extends ResultLogger {

  /**
   * Creates a new truststore and puts given certificate inside
   *
   * @param password     truststore password
   * @param certificate     to put inside truststore
   * @param certificateType e.g. X.509
   * @param truststoreType  e.g. JKS
   * @param truststorePath  to write truststore to
   * @return truststore password
   */
  @SuppressWarnings(Array("AsInstanceOf", "NullParameter"))
  def createTruststoreWithGivenCert(
    password: String,
    certificate: Array[Byte],
    certificateType: String,
    truststoreType: String,
    truststorePath: String
  ): Unit = {
    logger.info("Creating truststore of type \"{}\" on path \"{}\"...", truststoreType: Any, truststorePath: Any)
    val ks = KeyStore.getInstance(truststoreType)
    val x509Certificate =
      CertificateFactory
        .getInstance(certificateType)
        .generateCertificate(new ByteArrayInputStream(certificate))
        .asInstanceOf[X509Certificate]

    ks.load(null)
    ks.setCertificateEntry("ca-cert", x509Certificate)

    val os: OutputStream = java.nio.file.Files.newOutputStream(Paths.get(truststorePath))
    try {
      ks.store(os, password.toCharArray)
    } finally {
      os.close()
      logger.info("Truststore created.")
    }
  }

  def createTruststoreOnExecutors(spark: SparkSession,
                                  certDataPass: Option[(String, String, String)]): Unit = certDataPass match {
    case Some((certificate, password, nodePath)) =>
      val createTruststoreWithIterator = createTruststoreFunc(password, certificate, nodePath)

      createTruststoreWithIterator.apply(Iterator[Int]()) // run on driver
      val numExecutors = Integer.parseInt(spark.sparkContext.getConf.get("spark.executor.instances", "2"))
      spark.sparkContext
        .parallelize(1 to (numExecutors * 100))
        .repartition(numExecutors * 10)
        .foreachPartition(createTruststoreWithIterator)
    case _ =>
  }

  private def createTruststoreFunc(password: String,
                                   certificate: String,
                                   truststorePath: String
                                  ): Iterator[Int] => Unit =
    _ => TruststoreGenerator.createTruststoreWithGivenCert(
      password,
      certificate = Base64.getDecoder.decode(certificate),
      certificateType = "X.509",
      truststoreType = "JKS",
      truststorePath = truststorePath
    )
}
