package cn.aipm.image

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.Config
import cn.pidb.util.ConfigEx._



class DogOrCatClassifier extends PropertyExtractor {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  override def declareProperties() = Map("animal" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val animal = Services.initialize(aipmHttpHostUrl).classifyAnimal(is)
    Map("animal" -> animal)
  })

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }
}
