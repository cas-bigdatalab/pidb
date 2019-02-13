package cn.pidb.blob

trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def argumentType: ValueType;

  def extract(value: Any): Map[String, Any];
}

trait ValueComparator {
  /**
    * @return 0~1
    */
  def compare(a: Any, b: Any): Double;

  def argumentTypes: (ValueType, ValueType);
}

trait ValueType {
}

case class BlobValueType(val mimeMajorTypeName: String)
  extends ValueType {

}

case class JavaValueType(val javaTypeName: String)
  extends ValueType {
}

case class AnyValueType()
  extends ValueType {
}

object ValueType {
  def javaType[T](implicit m: Manifest[T]) = JavaValueType(m.runtimeClass.getSimpleName.toLowerCase);

  val ANY = new AnyValueType();

  val ANY_BLOB = mimeType("");

  def mimeType(majorName: String) = BlobValueType(majorName.toLowerCase())

  def of(o: Any) = {
    o match {
      case n: Blob => mimeType(n.mimeType.major)
      case _ => JavaValueType(o.getClass.getSimpleName.toLowerCase())
    }
  }
}