package cn.aipm

import cn.pidb.blob.{Blob, PropertyExtractor}
import cn.pidb.util.Config

/**
  * Created by bluejoe on 2019/2/17.
  */
class CommonPropertyExtractor extends PropertyExtractor {
  override def declareProperties() = Map("class" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = {
    Map("class" -> x.getClass)
  }

  override def initialize(conf: Config) {
  }
}

class CommonBlobPropertyExtractor extends PropertyExtractor {
  override def declareProperties() = Map("length" -> classOf[Int], "mime" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = {
    x match {
      case b: Blob => Map("length" -> b.length, "mime" -> b.mimeType.text)
    }
  }

  override def initialize(conf: Config) {
  }
}