package cn.pidb.engine.blob

import cn.pidb.blob._

import scala.beans.BeanProperty

/**
  * Created by bluejoe on 2019/1/31.
  */
class CypherPluginRegistry {
  @BeanProperty var extractors: Array[PropertyExtractor] = Array();
  @BeanProperty var comparators: Array[ValueComparator] = Array();
  @BeanProperty var thresholds: Array[ThresholdEntry] = Array();
}

class ThresholdEntry {
  @BeanProperty var source: String = null;
  @BeanProperty var target: String = null;
  @BeanProperty var threshold: Double = 0.7;
}


trait CustomPropertyProvider {
  def getCustomProperty(x: Any, propertyName: String): Any;
}

class CustomPropertyExtractorRegistry(list: Array[PropertyExtractor]) extends CustomPropertyProvider {
  val map: Map[(String, ValueType), Array[PropertyExtractor]] = list
    .flatMap(x => (x.declareProperties().map(prop => (prop._1, x.argumentType) -> x)))
    .groupBy(_._1)
    .map(x => x._1 -> x._2.map(_._2))

  //TODO: cache extraction
  def getCustomProperty(x: Any, name: String): Any = {
    name match {
      case _ =>
        val m1 = map.get(name -> ValueType.of(x));
        m1.map(_.head.extract(x).apply(name))
          .getOrElse {
            val m2 =
              if (x.isInstanceOf[Blob])
                map.get(name -> ValueType.ANY_BLOB)
              else
                None;

            m2.map(_.head.extract(x).apply(name))
              .getOrElse {
                map.get(name -> ValueType.ANY)
                  .map(_.head.extract(x).apply(name))
                  .getOrElse {
                    throw new UnknownPropertyException(name, x);
                  }
              }
          }
    }
  }
}

trait ValueMatcher {
  def like(a: Any, b: Any, threshold: Double): Boolean;

  def compare(a: Any, b: Any): Double;
}

class ValueComparatorRegistry(list: Array[ValueComparator]) extends ValueMatcher {
  val map: Map[(ValueType, ValueType), ValueComparator] = list.map(x => x.argumentTypes -> x)
    .map(x => (x._1._1 -> x._1._2) -> x._2)
    .toMap

  def like(a: Any, b: Any, threshold: Double): Boolean =
    compare(a, b) > threshold

  override def compare(a: Any, b: Any): Double = {
    (a, b) match {
      case (null, null) => 1.0
      case _ => map.get(ValueType.of(a) -> ValueType.of(b)).map(x => x.compare(a, b)).getOrElse(throw new NoSuitableComparatorException(a, b))
    }
  }
}

class UnknownPropertyException(name: String, x: Any)
  extends RuntimeException(s"unknown property `$name` for $x") {

}

class NoSuitableComparatorException(a: Any, b: Any)
  extends RuntimeException(s"do not know how to compare: ${a.getClass.getSimpleName} and ${b.getClass.getSimpleName}") {

}