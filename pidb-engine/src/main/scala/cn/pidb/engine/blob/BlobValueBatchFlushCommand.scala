package cn.pidb.engine.blob

import cn.pidb.blob.storage.BlobStorage
import cn.pidb.engine.{BlobPropertyStoreService, BlobPropertyStoreServiceImpl}
import cn.pidb.engine.blob.extensions.{GraphServiceContext, TransactionRecordStateExtension}
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.api.CommandVisitor
import org.neo4j.kernel.impl.store.NeoStores
import org.neo4j.kernel.impl.transaction.command.Command
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState
import org.neo4j.storageengine.api.WritableChannel
import org.neo4j.storageengine.api.txstate.ReadableTransactionState

/**
  * Created by bluejoe on 2019/2/19.
  */
class BlobValueBatchFlushCommand(val neoStores: NeoStores, val txState: ReadableTransactionState, val recordState: TransactionRecordState)
  extends Command {
  override def handle(handler: CommandVisitor): Boolean = {
    //println(handler);
    val conf = neoStores._get("config").asInstanceOf[Config];
    val spss: BlobPropertyStoreService = conf.asInstanceOf[GraphServiceContext].getBlobPropertyStoreService;
    recordState.asInstanceOf[TransactionRecordStateExtension].flushBlobs(spss);
    true;
  }

  override def serialize(channel: WritableChannel): Unit = {
    //println(channel);
  }

  override def toString() = "";
}
