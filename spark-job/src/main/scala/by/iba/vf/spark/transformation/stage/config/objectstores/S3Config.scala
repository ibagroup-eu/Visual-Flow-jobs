package by.iba.vf.spark.transformation.stage.config.objectstores

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.utils.ConnectionUtils
import org.apache.spark.sql.SparkSession

class S3Config(config: Node) extends BaseS3LikeStorageConfig(config) {
  final private val signed: Boolean = config.value(fieldAnonymousAccess).toBoolean
  final private val endpoint = config.value(fieldEndpoint)
  final private val ssl = config.value(fieldSsl)
  final private val pathStyle = (!config.value(fieldSsl).toBoolean).toString
  val options: Map[String, String] = ConnectionUtils.buildFormatOptions(config, format)

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

object S3Config extends ObjectStorageS3LikeConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.get(fieldStorage).contains(s3Storage) &&
      config.contains(fieldAnonymousAccess) &&
      config.contains(fieldEndpoint) &&
      config.contains(fieldSsl)
}