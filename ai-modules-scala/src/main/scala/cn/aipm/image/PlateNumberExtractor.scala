package cn.aipm.image

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.Config


class PlateNumberExtractor extends PropertyExtractor {
  override def declareProperties() = Map("plateNumber" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val plateNumber = Services.extractPlateNumber(is)
    Map("plateNumber" -> plateNumber)
  })

  override def initialize(conf: Config): Unit = {

  }
}