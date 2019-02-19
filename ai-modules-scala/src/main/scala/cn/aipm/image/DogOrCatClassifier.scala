package cn.aipm.image

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.aipm.service.Services
import cn.pidb.util.Config


class DogOrCatClassifier extends PropertyExtractor {
  override def declareProperties() = Map("Animal" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val animal = Services.classifyAnimal(is)
    Map("animal" -> animal)
  })

  override def initialize(conf: Config): Unit = {

  }
}