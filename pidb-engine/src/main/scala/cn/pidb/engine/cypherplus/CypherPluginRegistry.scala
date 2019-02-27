package cn.pidb.engine.cypherplus

import cn.pidb.blob._
import cn.pidb.util.{Config, Logging}

import scala.beans.BeanProperty

/**
  * Created by bluejoe on 2019/1/31.
  */
trait CustomPropertyProvider {
  def getCustomProperty(x: Any, propertyName: String): Option[Any];
}

trait ValueMatcher {
  def like(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  def containsOne(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  def containsSet(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  /**
    * compares two values
    */
  def compareOne(a: Any, b: Any, algoName: Option[String]): Option[Double];

  /**
    * compares two objects as sets
    */
  def compareSet(a: Any, b: Any, algoName: Option[String]): Option[Array[Array[Double]]];
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
  //default threshold
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
    def getCustomProperty(x: Any, name: String): Option[Any] = {
      //image:png
      val m1 = map.get(name -> ValueType.typeNameOf(x)).map(_.head.extract(x).apply(name));

      if (m1.isDefined) {
        m1
      }
      else {
        //blob:*
        val m3 =
          if (x.isInstanceOf[Blob]) {
            map.get(name -> ValueType.ANY_BLOB).map(_.head.extract(x).apply(name))
          }
          else {
            None
          }

        if (m3.isDefined) {
          m3
        }
        else {
          //*
          map.get(name -> ValueType.ANY)
            .map(_.head.extract(x).apply(name))
        }
      }
    }
  }

  def createValueComparatorRegistry(conf: Config) = new ValueMatcher with Logging {
    //initialize all comparators
    comparators.flatMap(_.valueComparators).map(_.comparator).foreach(_.initialize(conf));
    comparators.flatMap(_.setComparators).map(_.comparator).foreach(_.initialize(conf));

    def like(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(false)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          Some(m.apply(a, b) > threshold.getOrElse(e.threshold))
      }
    }

    override def compareOne(a: Any, b: Any, algoName: Option[String]): Option[Double] = {
      (a, b) match {
        case (null, null) => Some(1.0)
        case (null, _) => Some(0.0)
        case (_, null) => Some(0.0)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          Some(m.apply(a, b))
      }
    }

    override def compareSet(a: Any, b: Any, algoName: Option[String]): Option[Array[Array[Double]]] = {
      (a, b) match {
        case (null, null) => Some(Array(Array(1.0)))
        case (null, _) => Some(Array(Array(0.0)))
        case (_, null) => Some(Array(Array(0.1)))
        case _ =>
          val (m: CompareSetMethod, e: DomainComparatorEntry) = getNotNullSetComparator(a, b, algoName)
          Some(m.apply(a, b))
      }
    }

    override def containsOne(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(true)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          val r = m.apply(a, b)
          val th = threshold.getOrElse(e.threshold)
          Some(r > th)
      }
    }

    override def containsSet(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(true)
        case _ =>
          val (m: CompareSetMethod, e: DomainComparatorEntry) = getNotNullSetComparator(a, b, algoName)
          val r = m.apply(a, b)
          val th = threshold.getOrElse(e.threshold)
          Some(!r.exists(_.max <= th))
      }
    }

    type CompareSetMethod = (Any, Any) => Array[Array[Double]];
    type CompareValueMethod = (Any, Any) => Double;

    private def getRegisteredComparatorEntry(a: Any, b: Any): (DomainComparatorEntry, Boolean) = {
      comparators.find(_.domain.equalsIgnoreCase(ValueType.typeNameOf(a, b))).map(_ -> false)
        .getOrElse(comparators.find(_.domain.equalsIgnoreCase(ValueType.typeNameOf(b, a))).map(_ -> true)
          .getOrElse(throw new NoSuitableComparatorException(a, b)))
    }

    private def getRegisteredValueComparator(a: Any, b: Any, algoName: Option[String]): Option[(CompareValueMethod, DomainComparatorEntry)] = {
      //TODO: use cache
      val (entry, swap) = getRegisteredComparatorEntry(a, b);

      algoName.map(name =>
        entry.valueComparators.find(_.name.equalsIgnoreCase(name))
          .orElse(throw new UnknownAlgorithmException(name)))
        .getOrElse(entry.valueComparators.headOption)
        .map { x: NamedValueComparatorEntry =>
          (if (swap) {
            (a: Any, b: Any) => x.comparator.compare(b, a)
          }
          else {
            (a: Any, b: Any) => x.comparator.compare(a, b)
          })
        }.map(_ -> entry)
    }

    private def getRegisteredSetComparator(a: Any, b: Any, algoName: Option[String]): Option[(CompareSetMethod, DomainComparatorEntry)] = {
      //TODO: use cache
      val (entry, swap) = getRegisteredComparatorEntry(a, b);

      algoName.map(name =>
        entry.setComparators.find(_.name.equalsIgnoreCase(name))
          .orElse(throw new UnknownAlgorithmException(name)))
        .getOrElse(entry.setComparators.headOption)
        .map { x: NamedSetComparatorEntry =>
          (if (swap) {
            (a: Any, b: Any) => x.comparator.compare(b, a)
          }
          else {
            (a: Any, b: Any) => x.comparator.compare(a, b)
          })
        }.map(_ -> entry)
    }

    private def getNotNullValueComparator(a: Any, b: Any, algoName: Option[String]): (CompareValueMethod, DomainComparatorEntry) = {
      //TODO: use cache
      val opt = getRegisteredValueComparator(a, b, algoName);
      opt.orElse(
        getRegisteredSetComparator(a, b, algoName)
          .orElse(throw new NoSuitableComparatorException(a, b))
          .map(en => asCompareValueMethod(en._1.asInstanceOf[CompareSetMethod]) -> en._2))
        .get
    }

    private def getNotNullSetComparator(a: Any, b: Any, algoName: Option[String]): (CompareSetMethod, DomainComparatorEntry) = {
      //TODO: use cache
      val opt = getRegisteredSetComparator(a, b, algoName);
      opt.orElse(
        getRegisteredValueComparator(a, b, algoName)
          .orElse(throw new NoSuitableComparatorException(a, b))
          .map(en => asCompareSetMethod(en._1.asInstanceOf[CompareValueMethod]) -> en._2))
        .get
    }

    private def asCompareValueMethod(m: CompareSetMethod): CompareValueMethod = {
      (a: Any, b: Any) =>
        val r: Array[Array[Double]] = m(a, b);
        r.flatMap(x => x).max;
    }

    private def asCompareSetMethod(m: CompareValueMethod): CompareSetMethod = {
      (a: Any, b: Any) =>
        val r: Double = m(a, b);
        Array(Array(r))
    }
  }
}

class UnknownPropertyException(name: String, x: Any)
  extends RuntimeException(s"unknown property `$name` for $x") {

}

class NoSuitableComparatorException(a: Any, b: Any)
  extends RuntimeException(s"no suiltable comparator: ${ValueType.typeNameOf(a)} and ${ValueType.typeNameOf(b)}") {

}

class UnknownAlgorithmException(name: String)
  extends RuntimeException(s"unknown algorithm: $name") {

}

class TooManyObjectsException(o: Any)
  extends RuntimeException(s"too many objects: $o") {

}