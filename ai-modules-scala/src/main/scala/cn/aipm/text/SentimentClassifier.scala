package cn.aipm.text

import cn.pidb.blob.PropertyExtractor
import cn.aipm.service.Services
import cn.pidb.util.Configuration
import cn.pidb.util.ConfigurationEx._

class SentimentClassifier extends PropertyExtractor {

  var aipmHttpHostUrl = "http://127.0.0.1/"
  override def declareProperties() = Map("sentiment" -> classOf[String])

  override def extract(text: Any): Map[String, Any] = {

    val sentiment = Services.initialize(aipmHttpHostUrl).sentimentClassifier(text.asInstanceOf[String])
    Map("sentiment" -> sentiment)
  }

  override def initialize(conf: Configuration): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }

}

