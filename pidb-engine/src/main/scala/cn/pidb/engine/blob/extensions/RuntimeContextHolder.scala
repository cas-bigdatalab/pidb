package cn.pidb.engine.blob.extensions

import scala.collection.mutable.{Map => MMap}

/**
  * Created by bluejoe on 2018/8/12.
  */
class RuntimeContextHolder {
  private val _map = MMap[String, Any]();

  def getRuntimeContextMap() = _map;

  def putRuntimeContext(key: String, value: Any) = _map(key) = value;

  def putRuntimeContext[T](value: Any)(implicit manifest: Manifest[T]) = _map(manifest.runtimeClass.getName) = value;

  def getRuntimeContext(key: String): Any = _map(key);

  def getRuntimeContext[T]()(implicit manifest: Manifest[T]): T = _map(manifest.runtimeClass.getName).asInstanceOf[T];
}