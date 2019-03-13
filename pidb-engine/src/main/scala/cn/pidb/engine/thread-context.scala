package cn.pidb.engine

import cn.pidb.blob.storage.BlobStorage
import cn.pidb.blob.{Blob, BlobId}
import cn.pidb.engine.blob.extensions.RuntimeContext
import cn.pidb.util.Logging
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.api.KernelTransaction

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2018/11/28.
  */
object ThreadBoundContext extends Logging {
  private val _stateLocal: ThreadLocal[BoundTransactionState] = new ThreadLocal[BoundTransactionState]();

  def state: BoundTransactionState = _stateLocal.get;

  def bindState(state: BoundTransactionState) = _stateLocal.set(state);

  def unbindState() = _stateLocal.remove();

  def bind(tx: KernelTransaction) = bindState(new BoundTransactionState() {
    override def conf: RuntimeContext = tx._get("storageEngine.neoStores.config").asInstanceOf[RuntimeContext];
  });

  def blobBuffer: TransactionalBlobBuffer = state.blobBuffer;

  def conf: RuntimeContext = state.conf;

  def streamingBlobs: CachedStreamingBlobList = state.streamingBlobs;

  def blobPropertyStoreService: BlobPropertyStoreService = state.blobPropertyStoreService;

  def blobStorage: BlobStorage = state.blobStorage;
}

trait BoundTransactionState {
  def conf: RuntimeContext;

  def blobPropertyStoreService: BlobPropertyStoreService = conf.getBlobPropertyStoreService;

  def blobStorage: BlobStorage = blobPropertyStoreService.blobStorage;

  def blobBuffer: TransactionalBlobBuffer = new TransactionalBlobBufferImpl(blobStorage);

  def streamingBlobs: CachedStreamingBlobList = new CachedStreamingBlobListImpl();
}

trait CachedStreamingBlobList {
  def addBlob(id: String);

  def ids(): Iterable[String];
}

class CachedStreamingBlobListImpl() extends CachedStreamingBlobList {
  private val _ids = ArrayBuffer[String]();

  def addBlob(id: String) = {
    _ids += id;
  }

  def ids() = {
    val a = _ids.toArray;
    _ids.clear();
    a;
  }
}

trait TransactionalBlobBuffer {
  def addBlob(id: BlobId, blob: Blob);

  def removeBlob(bid: BlobId);

  def flushBlobs(): Unit;

  def getBlob(id: BlobId): Option[Blob];
}

class TransactionalBlobBufferImpl(blobStorage: BlobStorage) extends TransactionalBlobBuffer with Logging {

  trait BlobChange {
    val id: BlobId;
    val idString = id.asLiteralString();
  }

  case class BlobAdd(id: BlobId, blob: Blob) extends BlobChange {

  }

  case class BlobDelete(id: BlobId) extends BlobChange {
  }

  val addedBlobs = ArrayBuffer[BlobChange]();

  val deletedBlobs = ArrayBuffer[BlobChange]();

  def getBlob(id: BlobId): Option[Blob] = {
    val sid = id.asLiteralString();
    addedBlobs.find(x => x.isInstanceOf[BlobAdd]
      && x.idString.equals(sid))
      .map(_.asInstanceOf[BlobAdd].blob)
  }

  def addBlob(id: BlobId, blob: Blob) = addedBlobs += BlobAdd(id, blob)

  def removeBlob(bid: BlobId) = deletedBlobs += BlobDelete(bid);

  private def flushAddedBlobs(): Unit = {
    addedBlobs.filter(_.isInstanceOf[BlobAdd]).map(_.asInstanceOf[BlobAdd])
      .map(x => x.id -> x.blob).grouped(100).foreach(ops => {
      blobStorage.saveBatch(ops);
      logger.debug(s"blobs saved: [${ops.map(_._2).mkString(", ")}]");
    }
    )

    addedBlobs.clear()
  }

  private def flushDeletedBlobs(): Unit = {
    val deleted = deletedBlobs.filter(_.isInstanceOf[BlobDelete]).map(_.asInstanceOf[BlobDelete].id);

    if (deleted.nonEmpty) {
      blobStorage.deleteBatch(deleted);
      logger.debug(s"blobs deleted: [${deleted.map(_.asLiteralString()).mkString(", ")}]");
    }

    deletedBlobs.clear()
  }

  def flushBlobs() = {
    flushAddedBlobs();
    flushDeletedBlobs();
  }
}