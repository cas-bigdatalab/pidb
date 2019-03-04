package cn.pidb.blob

import cn.pidb.util.Configuration

trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def initialize(conf: Configuration);

  def extract(value: Any): Map[String, Any];
}

trait AnyComparator {
  def initialize(conf: Configuration);
}

trait ValueComparator extends AnyComparator {
  def compare(a: Any, b: Any): Double;
}

trait SetComparator extends AnyComparator {
  def compareAsSets(a: Any, b: Any): Array[Array[Double]];
}