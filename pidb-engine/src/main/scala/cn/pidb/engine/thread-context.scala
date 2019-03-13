package cn.pidb.engine

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
  private def _bind[T](value: T, local: ThreadLocal[T]) = {
    local.set(value);
  }

  private def _get[T](local: ThreadLocal[T]) = {
    local.get();
  }

  private val _stateLocal: ThreadLocal[BoundTransactionState] = new ThreadLocal[BoundTransactionState]();

  def state: BoundTransactionState = _get(_stateLocal);

  def bindState(state: BoundTransactionState) = _bind(state, _stateLocal);

  def bind(tx: KernelTransaction) = bindState(new BoundTransactionState() {
    override val conf: RuntimeContext = tx._get("storageEngine.neoStores.config").asInstanceOf[RuntimeContext];
    override val blobBuffer: TransactionalBlobBuffer = new TransactionalBlobBufferImpl(conf.getBlobPropertyStoreService);
    val streamingBlobs: CachedStreamingBlobList = new CachedStreamingBlobListImpl();
  });

  def blobBuffer: TransactionalBlobBuffer = state.blobBuffer;

  def conf: RuntimeContext = state.conf;

  def streamingBlobs: CachedStreamingBlobList = state.streamingBlobs;

  def blobPropertyStoreService: BlobPropertyStoreService = conf.getBlobPropertyStoreService;
}

trait BoundTransactionState {
  val conf: RuntimeContext;
  val blobBuffer: TransactionalBlobBuffer;
  val streamingBlobs: CachedStreamingBlobList;
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

class TransactionalBlobBufferImpl(bpss: BlobPropertyStoreService) extends TransactionalBlobBuffer with Logging {

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

  private def flushAddedBlobs(bpss: BlobPropertyStoreService): Unit = {
    addedBlobs.filter(_.isInstanceOf[BlobAdd]).map(_.asInstanceOf[BlobAdd])
      .map(x => x.id -> x.blob).grouped(100).foreach(ops => {
      bpss.blobStorage.saveBatch(ops);
      logger.debug(s"blobs saved: [${ops.map(_._2).mkString(", ")}]");
    }
    )

    addedBlobs.clear()
  }

  private def flushDeletedBlobs(bpss: BlobPropertyStoreService): Unit = {
    val deleted = deletedBlobs.filter(_.isInstanceOf[BlobDelete]).map(_.asInstanceOf[BlobDelete].id);

    if (deleted.nonEmpty) {
      bpss.blobStorage.deleteBatch(deleted);
      logger.debug(s"blobs deleted: [${deleted.map(_.asLiteralString()).mkString(", ")}]");
    }

    deletedBlobs.clear()
  }

  def flushBlobs() = {
    flushAddedBlobs(bpss);
    flushDeletedBlobs(bpss);
  }
}