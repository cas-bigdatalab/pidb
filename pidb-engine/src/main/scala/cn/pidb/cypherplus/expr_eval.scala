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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import cn.pidb.engine.{BlobPropertyStoreService, RuntimeContextHolder}
import cn.pidb.util.ReflectUtils._
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

case class CustomProperty(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable {
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = mapExpr(ctx, state) match {
    case n if n == Values.NO_VALUE => Values.NO_VALUE

    case x: Value =>
      val pv = state._get("query.inner.transactionalContext.tc.graph.graph.config").asInstanceOf[RuntimeContextHolder]
        .getRuntimeContext[BlobPropertyStoreService].getCustomPropertyProvider
        .getCustomProperty(x.asObject, propertyKey.name)

      Values.unsafeOf(pv, true);
  }

  ////end of NOTE

  def rewrite(f: (Expression) => Expression) = f(CustomProperty(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def symbolTableDependencies = mapExpr.symbolTableDependencies

  override def toString = s"$mapExpr.${propertyKey.name}"
}

case class ValueLikeExpression(lhsExpr: Expression, threshold: Double, rightExpr: Expression)
                              (implicit converter: TextValue => TextValue = identity) extends Predicate {
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val lValue = lhsExpr(m, state)
    val rValue = rightExpr(m, state)
    (lValue, rValue) match {
      case (Values.NO_VALUE, Values.NO_VALUE) => None
      case (BlobValue(x), BlobValue(y)) => {
        (x.length, y.length, x.mimeType.code == y.mimeType.code) match {
          case (0, 0, _) => Some(true)
          case (0, _, _) => Some(false)
          case (_, 0, _) => Some(false)
          case (_, _, false) => Some(false)
          case _ => Some(state._get("query.inner.transactionalContext.tc.graph.graph.config").
            asInstanceOf[RuntimeContextHolder].getRuntimeContext[BlobPropertyStoreService]()
            .getValueMatcher.like(x, y, threshold))
        }
      }
      case (BlobValue(x), y: Value) => {
        (x.length) match {
          case (0) => Some(false)
          case _ => Some(state._get("query.inner.transactionalContext.tc.graph.graph.config").
            asInstanceOf[RuntimeContextHolder].getRuntimeContext[BlobPropertyStoreService]()
            .getValueMatcher.like(x, y.asObject(), threshold))
        }
      }
      case (lhs, rhs) =>
        throw new InvalidLikeException(rhs)
    }
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => ValueLikeExpression(lhsExpr.rewrite(f), threshold, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

case class ValueCompareExpression(lhsExpr: Expression, algorithm: String, rightExpr: Expression)
                                 (implicit converter: TextValue => TextValue = identity) extends Expression {
  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val aVal = lhsExpr(ctx, state)
    val bVal = rightExpr(ctx, state)

    (aVal, bVal) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
      case (a:Value,b:Value) => compare(a, b, state)
    }
  }

  private def compare(b1: Value, b2: Value, state: QueryState): Value = {
    Values.doubleValue(state._get("query.inner.transactionalContext.tc.graph.graph.config").asInstanceOf[RuntimeContextHolder]
      .getRuntimeContext[BlobPropertyStoreService].getValueMatcher.compare(b1.asObject, b2.asObject()))
  }

  override def toString: String = lhsExpr.toString() + " %% /" + rightExpr.toString() + "/"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(rightExpr.rewrite(f) match {
    case other => ValueCompareExpression(lhsExpr.rewrite(f), algorithm, other)(converter)
  })

  def arguments = Seq(lhsExpr, rightExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rightExpr.symbolTableDependencies
}

class InvalidLikeException(compared: AnyValue) extends RuntimeException {

}