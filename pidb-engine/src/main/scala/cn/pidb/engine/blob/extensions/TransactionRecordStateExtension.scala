package cn.pidb.engine.blob.extensions

import cn.pidb.blob.{Blob, BlobId}
import cn.pidb.engine.BlobStoreService$
import cn.pidb.util.Logging

import scala.collection.mutable.ArrayBuffer

class TransactionRecordStateExtension extends Logging {
  val blobChanges = ArrayBuffer[BlobChange]();

  def addBlob(id: BlobId, blob: Blob) = blobChanges += BlobAdd(id, blob)

  def flushBlobs(bpss: BlobPropertyStoreService): Unit = {
    blobChanges.filter(_.isInstanceOf[BlobAdd]).map(
      _ match {
        case BlobAdd(id, blob) => id -> blob
      }).grouped(100).foreach(ops => {
      bpss.blobStorage.saveBatch(ops);
      logger.debug(s"blobs saved: $ops");
    }
    )

    blobChanges.clear()
  }
}

trait BlobChange {
}

case class BlobAdd(id: BlobId, blob: Blob) extends BlobChange {
}

case class BlobDelete(id: BlobId) extends BlobChange {
}