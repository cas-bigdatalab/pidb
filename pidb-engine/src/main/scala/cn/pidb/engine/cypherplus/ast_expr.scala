package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.{expressions => ast}

case class ValueLike(lhs: Expression, threshold: Double, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = s"like/$threshold"
}

case class CustomProperty(map: Expression, propertyKey: PropertyKeyName)(val position: InputPosition) extends LogicalProperty {
  override def asCanonicalStringVal = s"${map.asCanonicalStringVal}.${propertyKey.asCanonicalStringVal}"
}

case class ValueCompare(lhs: Expression, algorithm: Option[String], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = s"compare"
}