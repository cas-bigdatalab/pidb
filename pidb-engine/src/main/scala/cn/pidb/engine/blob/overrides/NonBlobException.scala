package org.neo4j.driver.v1.exceptions

import org.neo4j.driver.v1.Value

/**
  * Created by bluejoe on 2018/11/24.
  */
class NonBlobException(value: Value) extends
  Neo4jException(s"expect a blob, but get a ${value.`type`.name}: $value") {
}