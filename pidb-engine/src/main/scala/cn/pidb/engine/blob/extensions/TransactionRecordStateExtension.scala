package cn.pidb.engine.blob.extensions

import cn.pidb.blob.{Blob, BlobId}
import cn.pidb.engine.BlobPropertyStoreService
import cn.pidb.util.Logging

import scala.collection.mutable.ArrayBuffer

class TransactionalBlobBuffer extends Logging {
  val addedBlobs = ArrayBuffer[BlobChange]();

  val deletedBlobs = ArrayBuffer[BlobChange]();

  def prepare2AddBlob(id: BlobId, blob: Blob) = addedBlobs += BlobAdd(id, blob)

  def prepare2DeleteBlob(bid: BlobId) = deletedBlobs += BlobDelete(bid);

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
    if (!deleted.isEmpty) {
      //bpss.blobStorage.deleteBatch(deleted);
      logger.debug(s"blobs deleted: [${deleted.map(_.asLiteralString()).mkString(", ")}]");
    }

    deletedBlobs.clear()
  }
}

class TransactionalCachedStreams {
  val streamIds = ArrayBuffer[BlobId]();

  def add(id: BlobId) = streamIds += id;

  def ids = streamIds.toArray;
}

class TransactionRecordStateExtension {
  val committedBlobBuffer = new TransactionalBlobBuffer();
  val cachedStreams = new TransactionalCachedStreams();
}

trait BlobChange {
}

case class BlobAdd(id: BlobId, blob: Blob) extends BlobChange {
}

case class BlobDelete(id: BlobId) extends BlobChange {
}