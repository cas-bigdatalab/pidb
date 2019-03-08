package cn.pidb.engine

import cn.pidb.engine.blob.extensions.RuntimeContext
import cn.pidb.util.Logging
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState

/**
  * Created by bluejoe on 2018/11/28.
  */
object ThreadBoundContext extends Logging {
  private def _bind[T](value: T, local: ThreadLocal[T]) = {
    /*
    if (local.get() != null)
      throw new UnsupportedOperationException();
    */
    val src = local.get();
    logger.debug(s"${Thread.currentThread()}: ${pretty(src)}-->${pretty(value)}");
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

  def bindConf(value: Config) = _bind(value, configLocal);

  def bindRecordState(value: TransactionRecordState) = _bind(value, recordStateLocal);

  /*
  lazy val transaction = _get(transactionLocal);
  private val transactionLocal: ThreadLocal[KernelTransactionImplementation] = new ThreadLocal[KernelTransactionImplementation]();
  def bindTransaction(value: KernelTransactionImplementation) = _bind(value, transactionLocal);
  */
  private val configLocal: ThreadLocal[Config] = new ThreadLocal[Config]();

  //def config = _get(configLocal);

  //def recordState = _get(recordStateLocal);

  //def runtimeContext: RuntimeContext = configLocal.get().asInstanceOf[RuntimeContext];
  private val recordStateLocal: ThreadLocal[TransactionRecordState] = new ThreadLocal[TransactionRecordState]();
}