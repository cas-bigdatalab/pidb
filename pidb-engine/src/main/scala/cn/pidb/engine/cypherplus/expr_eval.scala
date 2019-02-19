/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.pidb.engine.cypherplus

import cn.pidb.engine.{BlobPropertyStoreService, RuntimeContextHolder}
import cn.pidb.util.ReflectUtils._
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

case class CustomPropertyExpression(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable {
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = mapExpr(ctx, state) match {
    case n if n == Values.NO_VALUE => Values.NO_VALUE

    case x: Value =>
      val pv = state._get("query.inner.transactionalContext.tc.graph.graph.config").asInstanceOf[RuntimeContextHolder]
        .getRuntimeContext[BlobPropertyStoreService].getCustomPropertyProvider
        .getCustomProperty(x.asObject, propertyKey.name)

      Values.unsafeOf(pv, true);
  }

  def rewrite(f: (Expression) => Expression) = f(CustomPropertyExpression(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def symbolTableDependencies = mapExpr.symbolTableDependencies

  override def toString = s"$mapExpr.${propertyKey.name}"
}

case class SemanticLikeExpression(lhsExpr: Expression, algorithm: Option[String], rightExpr: Expression)
                                 (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val lValue = lhsExpr(m, state)
    val rValue = rightExpr(m, state)
    (lValue, rValue) match {
      case (Values.NO_VALUE, Values.NO_VALUE) => Some(true)
      case (_, Values.NO_VALUE) => Some(false)
      case (Values.NO_VALUE, _) => Some(false)
      case (a: Value, b: Value) => Some(state._get("query.inner.transactionalContext.tc.graph.graph.config").
        asInstanceOf[RuntimeContextHolder].getRuntimeContext[BlobPropertyStoreService]()
        .getValueMatcher.like(a.asObject(), b.asObject(), algorithm))
    }
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => SemanticLikeExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

case class SemanticNarrowerExpression(lhsExpr: Expression, algorithm: Option[String], rightExpr: Expression)
                                     (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    new SemanticBroaderExpression(lhsExpr, algorithm, rightExpr)(converter).isMatch(m, state).map(!_)
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => SemanticNarrowerExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

case class SemanticBroaderExpression(lhsExpr: Expression, algorithm: Option[String], rightExpr: Expression)
                                    (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val lValue = lhsExpr(m, state)
    val rValue = rightExpr(m, state)
    (lValue, rValue) match {
      case (Values.NO_VALUE, Values.NO_VALUE) => Some(true)
      case (_, Values.NO_VALUE) => Some(false)
      case (Values.NO_VALUE, _) => Some(false)
      case (a: Value, b: Value) => Some(state._get("query.inner.transactionalContext.tc.graph.graph.config").
        asInstanceOf[RuntimeContextHolder].getRuntimeContext[BlobPropertyStoreService]()
        .getValueMatcher.contains(a.asObject(), b.asObject(), algorithm))
    }
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => SemanticBroaderExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

case class SemanticUnlikeExpression(lhsExpr: Expression, algorithm: Option[String], rightExpr: Expression)
                                   (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    new SemanticUnlikeExpression(lhsExpr, algorithm, rightExpr)(converter).isMatch(m, state).map(!_);
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => SemanticUnlikeExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

case class SemanticCompareExpression(lhsExpr: Expression, algorithm: Option[String], rightExpr: Expression)
                                    (implicit converter: TextValue => TextValue = identity) extends Expression {
  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val aVal = lhsExpr(ctx, state)
    val bVal = rightExpr(ctx, state)

    (aVal, bVal) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
      case (a: Value, b: Value) => Values.doubleValue(state._get("query.inner.transactionalContext.tc.graph.graph.config").asInstanceOf[RuntimeContextHolder]
        .getRuntimeContext[BlobPropertyStoreService].getValueMatcher.compare(a.asObject, b.asObject(), algorithm))
    }
  }

  override def toString: String = lhsExpr.toString() + " %% /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => SemanticCompareExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

class InvalidSemanticOperatorException(compared: AnyValue) extends RuntimeException {

}