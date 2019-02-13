package cn.pidb.engine

import java.io.File

import cn.pidb.blob._
import cn.pidb.engine.storage.{BlobStorage, Closable}
import cn.pidb.util.ConfigEx._
import cn.pidb.util.{Config, Logging}

/**
  * a blob storage employed by BufferedBlobStorage
  */
trait ExternalBlobStorage extends Closable {

  def deleteBatch(bids: Iterable[BlobId]);

  def saveBatch(blobs: Iterable[(BlobId, Blob)]);

  def containsBatch(bids: Iterable[BlobId]): Iterable[Boolean];

  def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource];
}

/**
  * a blob write buffer
  * write-consistency is required
  */
trait BlobStorageBuffer {
  def flush(): Unit;

  def put(bid: BlobId, blob: Blob);

  def remove(bid: BlobId);

  def isBuffered(bid: BlobId): Boolean;

  def get(bid: BlobId): Option[InputStreamSource];
}

/**
  * a BufferedBlobStorage is actually a BlobStorage with buffer
  */
class BufferedBlobStorage extends BlobStorage with Logging {
  var externalBlobStorage: ExternalBlobStorage = _
  var buffer: BlobStorageBuffer = _;

  def save(bid: BlobId, blob: Blob) = {
    buffer.put(bid, blob);
  }

  def exists(bid: BlobId): Boolean = buffer.isBuffered(bid) || externalBlobStorage.containsBatch(Array(bid)).head;

  def load(bid: BlobId): InputStreamSource = buffer.get(bid).getOrElse(externalBlobStorage.loadBatch(Array(bid)).head);

  override def initialize(storeDir: File, blobIdFactory: cn.pidb.blob.BlobIdFactory, conf: Config) = {
    val externalStorageClass = conf.getValueAsClass("blob.storage", classOf[FileBasedExternalBlobStorage])
    val externalBlobStorage = externalStorageClass.newInstance().asInstanceOf[ExternalBlobStorage];

    val bufferClass = conf.getValueAsClass("blob.buffer.class", classOf[FileBasedBlobOutputBuffer])
    val buffer = bufferClass.newInstance().asInstanceOf[BlobStorageBuffer];

    externalBlobStorage.initialize(storeDir, blobIdFactory, conf);

    logger.info(s"blob storage initialized: ${externalBlobStorage}");

    logger.info(s"buffer enabled: ${buffer}");
    //buffer?
    //buffer.flush?
    //start autoflush thread?
  }

  def disconnect() = {
    externalBlobStorage.disconnect();
    //buffer.flush?
  }

  override def delete(bid: BlobId): Unit = {
    if (buffer.isBuffered(bid)) {
      buffer.remove(bid);
    }
    else {
      externalBlobStorage.deleteBatch(Array(bid));
    }
  }
}

class FileBasedBlobOutputBuffer extends BlobStorageBuffer {
  override def flush(): Unit = ???

  override def isBuffered(bid: BlobId): Boolean = ???

  override def put(bid: BlobId, blob: Blob): Unit = ???

  override def get(bid: BlobId): Option[InputStreamSource] = ???

  override def remove(bid: BlobId): Unit = ???
}

class FileBasedExternalBlobStorage extends ExternalBlobStorage {
  override def deleteBatch(bids: Iterable[BlobId]): Unit = ???

  override def containsBatch(bids: Iterable[BlobId]): Iterable[Boolean] = ???

  override def saveBatch(blobs: Iterable[(BlobId, Blob)]): Unit = ???

  override def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource] = ???

  override def disconnect(): Unit = ???

  override def initialize(storeDir: File, blobIdFactory: cn.pidb.blob.BlobIdFactory, conf: Config): Unit = ???
}