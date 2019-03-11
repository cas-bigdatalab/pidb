package cn.pidb.engine.blob.extensions

import cn.pidb.blob.{Blob, BlobId}
import cn.pidb.engine.BlobPropertyStoreService
import cn.pidb.util.Logging
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.api.state.TxState

import scala.collection.mutable.ArrayBuffer

class TxStateExtension extends Logging {
  val addedBlobs = ArrayBuffer[BlobChange]();

  val deletedBlobs = ArrayBuffer[BlobChange]();

  def getBufferedBlob(id: BlobId): Option[Blob] = {
    val sid = id.asLiteralString();
    addedBlobs.find(x => x.isInstanceOf[BlobAdd]
      && x.idString.equals(sid))
      .map(_.asInstanceOf[BlobAdd].blob)
  }

  def addBlob(id: BlobId, blob: Blob) = addedBlobs += BlobAdd(id, blob)

  def removeBlob(bid: BlobId) = deletedBlobs += BlobDelete(bid);

  def flushAddedBlobs(bpss: BlobPropertyStoreService): Unit = {
    addedBlobs.filter(_.isInstanceOf[BlobAdd]).map(_.asInstanceOf[BlobAdd])
      .map(x => x.id -> x.blob).grouped(100).foreach(ops => {
      bpss.blobStorage.saveBatch(ops);
      logger.debug(s"blobs saved: [${ops.map(_._2).mkString(", ")}]");
    }
    )

    addedBlobs.clear()
  }

  def flushDeletedBlobs(bpss: BlobPropertyStoreService): Unit = {
    val deleted = deletedBlobs.filter(_.isInstanceOf[BlobDelete]).map(_.asInstanceOf[BlobDelete].id);

    if (deleted.nonEmpty) {
      bpss.blobStorage.deleteBatch(deleted);
      logger.debug(s"blobs deleted: [${deleted.map(_.asLiteralString()).mkString(", ")}]");
    }

    deletedBlobs.clear()
  }

  def flushBlobs(conf: Config) = {
    val bpss: BlobPropertyStoreService = conf.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
    flushAddedBlobs(bpss);
    flushDeletedBlobs(bpss);
  }
}

trait BlobChange {
  val id: BlobId;
  val idString = id.asLiteralString();
}

case class BlobAdd(id: BlobId, blob: Blob) extends BlobChange {

}

case class BlobDelete(id: BlobId) extends BlobChange {
}

object Extensions {

  class TransactionExt(tx: KernelTransaction) {
    def _conf = tx._get("storageEngine.neoStores.config").asInstanceOf[Config];

    def _state = tx._get("txState").asInstanceOf[TxState];

    def _stateOption: Option[TxState] = {
      val state = tx._get("txState");
      if (state != null) {
        Some(state.asInstanceOf[TxState])
      }
      else
        None
    }
  }

  implicit def transaction2Ext(tx: KernelTransaction) = new TransactionExt(tx);
}