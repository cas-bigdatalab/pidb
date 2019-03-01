package cn.pidb.blob

import cn.pidb.util.Config

trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def initialize(conf: Config);

  def extract(value: Any): Map[String, Any];
}

trait AnyComparator {
  def initialize(conf: Config);
}

trait ValueComparator extends AnyComparator {
  def compare(a: Any, b: Any): Double;
}

trait SetComparator extends AnyComparator {
  def compareAsSets(a: Any, b: Any): Array[Array[Double]];
}