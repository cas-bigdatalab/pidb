package cn.pidb.engine.blob

import org.neo4j.storageengine.api.{WritableChannel, StorageCommand}
import org.neo4j.storageengine.api.txstate.ReadableTransactionState

/**
  * Created by bluejoe on 2019/2/19.
  */
class BlobValueBatchFlushCommand(txState: ReadableTransactionState) extends StorageCommand {
  override def serialize(channel: WritableChannel): Unit = {

  }
}
