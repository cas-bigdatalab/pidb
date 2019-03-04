package cn.aipm.image

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.Configuration
import cn.pidb.util.ConfigurationEx._


class PlateNumberExtractor extends PropertyExtractor {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  override def declareProperties() = Map("plateNumber" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val plateNumber = Services.initialize(aipmHttpHostUrl).extractPlateNumber(is)
    Map("plateNumber" -> plateNumber)
  })

  override def initialize(conf: Configuration): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }
}