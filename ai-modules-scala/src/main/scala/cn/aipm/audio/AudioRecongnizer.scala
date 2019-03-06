package cn.aipm.audio

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.ServiceInitializer


class AudioRecongnizer extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("content" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val content = service.mandarinASR(is)
    Map("content" -> content)
  })

}
