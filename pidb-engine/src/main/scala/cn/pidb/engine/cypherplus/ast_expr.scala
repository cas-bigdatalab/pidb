package cn.pidb.engine.cypherplus

import java.io.File

import cn.pidb.blob.Blob
import org.apache.commons.codec.binary.Base64
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.{expressions => ast}

trait BlobURL {
  def asCanonicalString: String;

  def createBlob: Blob;
}

case class BlobLiteralExpr(value: BlobURL)(val position: InputPosition) extends Expression {
  override def asCanonicalStringVal = value.asCanonicalString
}

case class BlobFileURL(filePath: String) extends BlobURL {
  override def asCanonicalString = filePath

  def createBlob: Blob = Blob.fromFile(new File(filePath))
}

case class BlobBase64URL(base64: String) extends BlobURL {
  override def asCanonicalString = base64

  def createBlob: Blob = Blob.fromBytes(Base64.decodeBase64(base64))
}

case class BlobHttpURL(url: String) extends BlobURL {
  override def asCanonicalString = url

  def createBlob: Blob = Blob.fromHttpURL(url)
}

case class BlobFtpURL(url: String) extends BlobURL {
  override def asCanonicalString = url

  def createBlob: Blob = Blob.fromURL(url)
}

case class SemanticLike(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticUnlike(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class CustomProperty(map: Expression, propertyKey: PropertyKeyName)(val position: InputPosition) extends LogicalProperty {
  override def asCanonicalStringVal = s"${map.asCanonicalStringVal}.${propertyKey.asCanonicalStringVal}"
}

case class SemanticCompare(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = s"compare"
}

case class SemanticContain(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticElementOf(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

