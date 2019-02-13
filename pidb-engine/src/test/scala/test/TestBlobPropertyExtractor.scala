package test

import javax.imageio.ImageIO

import cn.pidb.blob._

/**
  * Created by bluejoe on 2019/1/28.
  */
class TestAnyPropertyExtractor extends PropertyExtractor {
  override def declareProperties() = Map("test1" -> classOf[Int], "test2" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = Map("test1" -> 1, "test2" -> "hello")

  override def argumentType() = ValueType.ANY
}

class TestBlobPropertyExtractor extends PropertyExtractor {
  override def declareProperties() = Map("length" -> classOf[Int], "mime" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = {
    x match {
      case b: Blob => Map("length" -> b.length, "mime" -> b.mimeType.text)
    }
  }

  override def argumentType() = ValueType.ANY_BLOB
}

class TestImageMetaDataExtractor extends PropertyExtractor {
  override def declareProperties() = Map("width" -> classOf[Int], "height" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream((is) => {
    val srcImage = ImageIO.read(is);
    Map("height" -> srcImage.getHeight(), "width" -> srcImage.getWidth());
  })

  override def argumentType() = ValueType.mimeType("image")
}

class TestImagePlateNumberExtractor extends PropertyExtractor {
  override def declareProperties() = Map("plateNumber" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream((is) => {
    val srcImage = ImageIO.read(is);
    Map("plateNumber" -> "京NB6666");
  })

  override def argumentType() = ValueType.mimeType("image")
}

class TestImageSimilarityComparator extends ValueComparator {
  def compare(blob1: Any, other: Any): Double = {
    0.9
  }

  override def argumentTypes(): (ValueType, ValueType) =
    ValueType.mimeType("image") -> ValueType.mimeType("image")
}

class TestImagePlateNumberComparator extends ValueComparator {
  def compare(blob1: Any, other: Any): Double = {
    if ("京NB6666".matches(other.asInstanceOf[String]))
      1.0
    else
      0.0
  }

  override def argumentTypes(): (ValueType, ValueType) =
    ValueType.mimeType("image") -> ValueType.javaType[String]
}

class TestString2StringComparator extends ValueComparator {
  def compare(a: Any, b: Any): Double = {
    if (a.asInstanceOf[String].replaceAll("\\s", "").equals(b.asInstanceOf[String].replaceAll("\\s", "")))
      0.9
    else
      0.0
  }

  override def argumentTypes(): (ValueType, ValueType) =
    ValueType.javaType[String] -> ValueType.javaType[String]
}

class TestAudioTextComparator extends ValueComparator {
  def compare(blob1: Any, other: Any): Double = {
    0.9
  }

  override def argumentTypes(): (ValueType, ValueType) =
    ValueType.mimeType("audio") -> ValueType.javaType[String]
}