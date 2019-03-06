package cn.pidb.engine.blob.extensions

import cn.pidb.engine.{BlobPropertyStoreService}

import scala.collection.mutable.{Map => MMap}

/**
  * Created by bluejoe on 2018/8/12.
  */
class RuntimeContext {
  private val _map = MMap[String, Any]();

  def contextPut(key: String, value: Any) = _map(key) = value;

  def contextPut[T](value: Any)(implicit manifest: Manifest[T]) = _map(manifest.runtimeClass.getName) = value;

  def contextGet(key: String): Any = _map(key);

  def contextGet[T]()(implicit manifest: Manifest[T]): T = _map(manifest.runtimeClass.getName).asInstanceOf[T];

  def getBlobPropertyStoreService: BlobPropertyStoreService = contextGet[BlobPropertyStoreService];
}