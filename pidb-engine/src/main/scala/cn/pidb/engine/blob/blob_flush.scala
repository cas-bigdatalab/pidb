package cn.pidb.engine.blob

import cn.pidb.engine.BlobPropertyStoreService
import cn.pidb.engine.blob.extensions.{RuntimeContext, TransactionRecordStateExtension}
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.api.{BatchTransactionApplier, CommandVisitor, TransactionApplier}
import org.neo4j.kernel.impl.store.{PropertyType, NeoStores}
import org.neo4j.kernel.impl.transaction.command.Command
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState
import org.neo4j.storageengine.api.txstate.ReadableTransactionState
import org.neo4j.storageengine.api.{CommandsToApply, WritableChannel}

/**
  * Created by bluejoe on 2019/2/19.
  */
class BlobCollectorCommand(val neoStores: NeoStores, val txState: ReadableTransactionState, val recordState: TransactionRecordState)
  extends Command {
  override def handle(handler: CommandVisitor): Boolean = {
    if (handler.isInstanceOf[BlobFlushTransactionApplier]) {
      handler.asInstanceOf[BlobFlushTransactionApplier].handleOnClose = { () =>
        val conf = neoStores._get("config").asInstanceOf[Config];
        val bpss: BlobPropertyStoreService = conf.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
        recordState.asInstanceOf[TransactionRecordStateExtension].transactionalBlobBuffer.flushAddedBlobs(bpss);

        val spss: BlobPropertyStoreService = conf.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
        recordState.asInstanceOf[TransactionRecordStateExtension].transactionalBlobBuffer.flushDeletedBlobs(bpss);
      }

      true
    }
    else {
      false
    }

  }

  override def serialize(channel: WritableChannel): Unit = {
  }

  override def toString() = "";
}

class BlobFlushTransactionAppliers() extends BatchTransactionApplier.Adapter {
  override def startTx(transaction: CommandsToApply): TransactionApplier = {
    new BlobFlushTransactionApplier();
  }
}

class BlobFlushTransactionApplier extends TransactionApplier.Adapter {
  var handleOnClose: () => Unit = () => {}

  override def close(): Unit = {
    handleOnClose();
  }
}
