package cn.aipm.image

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.{Config, ConfigEx}


class PlateNumberExtractor extends PropertyExtractor {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  override def declareProperties() = Map("plateNumber" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val plateNumber = Services.initialize(aipmHttpHostUrl).extractPlateNumber(is)
    Map("plateNumber" -> plateNumber)
  })

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = ConfigEx.config2Ex(conf).getRequiredValueAsString("aipm.http.host.url")
  }
}