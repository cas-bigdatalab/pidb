package cn.pidb.blob

import cn.pidb.util.Config

trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def initialize(conf: Config);

  def extract(value: Any): Map[String, Any];
}

trait ValueComparator {
  /**
    * @return 0~1
    */
  def compare(a: Any, b: Any): Double;

  def initialize(conf: Config);
}