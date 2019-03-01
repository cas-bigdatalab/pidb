package cn.aipm.text

import cn.aipm.service.Services
import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.pidb.util.Config
import cn.pidb.util.ConfigEx._

class ChineseTokenizer extends PropertyExtractor {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  override def declareProperties() = Map("words" -> classOf[Array[String]])

  override def extract(text: Any): Map[String, Array[String]] = {
    val words = Services.initialize(aipmHttpHostUrl).segmentText(text.asInstanceOf[String]).toArray
    Map("words" -> words)
  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }
}