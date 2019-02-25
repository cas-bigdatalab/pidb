package cn.pidb.engine.cypherplus

import cn.pidb.blob._
import cn.pidb.util.{Config, Logging}

import scala.beans.BeanProperty

/**
  * Created by bluejoe on 2019/1/31.
  */
trait CustomPropertyProvider {
  def getCustomProperty(x: Any, propertyName: String): Any;
}

trait ValueMatcher {
  def like(a: Any, b: Any, algoName: Option[String]): Boolean;

  def contains(a: Any, b: Any, algoName: Option[String]): Boolean;

  def compare(a: Any, b: Any, algoName: Option[String]): Double;
}

object ValueType {
  def typeNameOf(x: Any): String = x match {
    case b: Blob => s"blob/${b.mimeType.major}".toLowerCase()
    case _ => x.getClass.getSimpleName.toLowerCase()
  }

  val ANY_BLOB = "blob/*";
  val ANY = "*";

  def typeNameOf(a: Any, b: Any): String = s"${typeNameOf(a)}:${typeNameOf(b)}"
}

class DomainExtractorEntry {
  @BeanProperty var domain: String = "";
  @BeanProperty var extractor: PropertyExtractor = _;
}

class DomainComparatorEntry {
  @BeanProperty var domain: String = "";
  @BeanProperty var threshold: Double = 0.7;
  @BeanProperty var valueComparators: Array[NamedValueComparatorEntry] = Array();
  @BeanProperty var setComparators: Array[NamedSetComparatorEntry] = Array();
}

class NamedSetComparatorEntry {
  @BeanProperty var name: String = "";
  @BeanProperty var comparator: SetComparator = _;
}

class NamedValueComparatorEntry {
  @BeanProperty var name: String = "";
  @BeanProperty var comparator: ValueComparator = _;
}


class CypherPluginRegistry {
  @BeanProperty var extractors: Array[DomainExtractorEntry] = Array();
  @BeanProperty var comparators: Array[DomainComparatorEntry] = Array();

  def createCustomPropertyProvider(conf: Config) = new CustomPropertyProvider {
    extractors.foreach(_.extractor.initialize(conf));

    //propertyName, typeName
    val map: Map[(String, String), Array[PropertyExtractor]] = extractors
      .flatMap(x => (x.extractor.declareProperties().map(prop => (prop._1, x.domain) -> x.extractor)))
      .groupBy(_._1)
      .map(x => x._1 -> x._2.map(_._2))

    //TODO: cache extraction
    def getCustomProperty(x: Any, name: String): Any = {
      val m1 = map.get(name -> ValueType.typeNameOf(x));
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

  def createValueComparatorRegistry(conf: Config) = new ValueMatcher with Logging {
    //all value comparators
    val valueGroupedComparators = comparators.map { x =>
      (x.getDomain, (x.valueComparators.map(x => x.name -> x.comparator).toMap -> x))
    }.toMap

    comparators.flatMap(_.valueComparators).map(_.comparator).foreach(_.initialize(conf));

    //all set comparators
    val groupedSetComparators = comparators.map { x =>
      (x.getDomain, (x.setComparators.map(x => x.name -> x.comparator).toMap -> x))
    }.toMap

    comparators.flatMap(_.setComparators).map(_.comparator).foreach(_.initialize(conf));

    def like(a: Any, b: Any, algoName: Option[String]): Boolean = {
      _match(a, b, algoName) match {
        case (x, null, null) => x != 0.0
        case (d, _, entry) => d > entry.threshold
      }
    }

    private def _match(a: Any, b: Any, algoName: Option[String]): (Double, ValueComparator, DomainComparatorEntry) = {
      (a, b) match {
        case (null, null) => (1.0, null, null)
        case (null, _) => (0.0, null, null)
        case (_, null) => (0.0, null, null)
        case _ =>
          val (a2, b2, (map, entry)) = valueGroupedComparators.get(ValueType.typeNameOf(a, b))
            .map {
              (a, b, _)
            }.getOrElse {
            (b, a, valueGroupedComparators.get(ValueType.typeNameOf(b, a)).getOrElse(throw new NoSuitableComparatorException(a, b)))
          }

          val comparator = algoName.map(
            map.get(_)
              .getOrElse(throw new UnknownAlgorithmException(algoName.get)))
            .getOrElse(map.headOption.map(_._2)
              .getOrElse(throw new NoSuitableComparatorException(a, b)))

          (comparator.compare(a2, b2), comparator, entry)
      }
    }

    override def compare(a: Any, b: Any, algoName: Option[String]): Double = {
      _match(a, b, algoName)._1
    }

    override def contains(a: Any, b: Any, algoName: Option[String]): Boolean = {
      (a, b) match {
        case (null, null) => true
        case (null, _) => false
        case (_, null) => true
        case _ =>
          val (a2, b2, (map, entry)) = groupedSetComparators.get(ValueType.typeNameOf(a, b))
            .map {
              (a, b, _)
            }.getOrElse {
            (b, a, groupedSetComparators.get(ValueType.typeNameOf(b, a)).getOrElse(throw new NoSuitableComparatorException(a, b)))
          }

          val comparator = algoName.map(
            map.get(_)
              .getOrElse(throw new UnknownAlgorithmException(algoName.get)))
            .getOrElse(map.headOption.map(_._2)
              .getOrElse(throw new NoSuitableComparatorException(a, b)))

          comparator.contains(a2, b2)
      }
    }
  }
}

class UnknownPropertyException(name: String, x: Any)
  extends RuntimeException(s"unknown property `$name` for $x") {

}

class NoSuitableComparatorException(a: Any, b: Any)
  extends RuntimeException(s"no suiltable comparator: ${a.getClass.getSimpleName} and ${b.getClass.getSimpleName}") {

}

class UnknownAlgorithmException(name: String)
  extends RuntimeException(s"unknown algorithm: $name") {

}