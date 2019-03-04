package cn.aipm.audio

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.{ConfigurationEx, Configuration, ConfigurationEx$}


class AudioRecongnizer extends PropertyExtractor{
  var aipmHttpHostUrl = "http://127.0.0.1/"

  override def declareProperties() = Map("content" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val content = Services.initialize(aipmHttpHostUrl).mandarinASR(is)
    Map("content" -> content)
  })

  override def initialize(conf: Configuration): Unit = {
    aipmHttpHostUrl = ConfigurationEx.config2Ex(conf).getRequiredValueAsString("aipm.http.host.url")
  }
}
