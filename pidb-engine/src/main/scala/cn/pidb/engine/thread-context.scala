package cn.pidb.engine

import java.util.function.Supplier

import cn.pidb.util.Logging
import org.neo4j.kernel.api.KernelTransaction

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2018/11/28.
  */
object ThreadBoundContext extends Logging {
  private def _bind[T](value: T, local: ThreadLocal[T]) = {
    /*
    val src = local.get();
    logger.debug(s"${Thread.currentThread()}: ${pretty(src)}-->${pretty(value)}");
    */
    local.set(value);
  }

  private def pretty(o: Any): String = {
    if (null == o)
      "null"
    else
      s"${o.getClass.getSimpleName}@${o.hashCode()}"
  }

  private def _get[T](local: ThreadLocal[T]) = {

    if (local.get() == null)
      logger.debug(s"${Thread.currentThread()}: value is null");

    local.get();
  }

  private val transactionLocal: ThreadLocal[KernelTransaction] = new ThreadLocal[KernelTransaction]();

  def transaction = _get(transactionLocal);

  def bind(value: KernelTransaction) = _bind(value, transactionLocal);

  private val cachedBlobsLocal: ThreadLocal[ArrayBuffer[String]] = ThreadLocal.withInitial(new Supplier[ArrayBuffer[String]] {
    override def get(): ArrayBuffer[String] = ArrayBuffer()
  });

  def cachedBlobs: ArrayBuffer[String] = cachedBlobsLocal.get();
}