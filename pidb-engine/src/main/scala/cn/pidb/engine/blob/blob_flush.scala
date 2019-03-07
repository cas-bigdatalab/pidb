package cn.pidb.engine.blob

import cn.pidb.engine.blob.extensions.{RuntimeContext, TransactionRecordStateExtension}
import cn.pidb.engine.{BlobCacheInSession, BlobPropertyStoreService}
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.api.{BatchTransactionApplier, CommandVisitor, TransactionApplier}
import org.neo4j.kernel.impl.store.NeoStores
import org.neo4j.kernel.impl.transaction.command.Command
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState
import org.neo4j.storageengine.api.txstate.ReadableTransactionState
import org.neo4j.storageengine.api.{CommandsToApply, WritableChannel}

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/2/19.
  */
class RecordStateCollectorCommand(val neoStores: NeoStores, val txState: ReadableTransactionState, val recordState: TransactionRecordState)
  extends Command {
  override def handle(handler: CommandVisitor): Boolean = {
    if (handler.isInstanceOf[BlobFlushTransactionApplier]) {
      val conf = neoStores._get("config").asInstanceOf[Config];
      handler.asInstanceOf[BlobFlushTransactionApplier].bind(conf, recordState);
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
  val _recordStates = mutable.Set[(Config, TransactionRecordState)]();

  def bind(conf: Config, recordState: TransactionRecordState) =
    _recordStates += conf -> recordState;

  override def close(): Unit = {
    _recordStates.foreach(x => {
      val (conf, recordState) = x;

      val bpss: BlobPropertyStoreService = conf.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
      recordState.asInstanceOf[TransactionRecordStateExtension].committedBlobBuffer.flushAddedBlobs(bpss);
      recordState.asInstanceOf[TransactionRecordStateExtension].committedBlobBuffer.flushDeletedBlobs(bpss);

      //invalidate cached blobs
      conf.asInstanceOf[RuntimeContext].contextGetOption[BlobCacheInSession].
        foreach(_.invalidate(recordState.asInstanceOf[TransactionRecordStateExtension].cachedStreams.ids));
    }
    )
  }
}
