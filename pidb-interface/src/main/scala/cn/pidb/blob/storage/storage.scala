package cn.pidb.blob.storage

import java.io.File

import cn.pidb.blob._
import cn.pidb.util.Configuration

trait RollbackCommand {
  def perform();
}

trait BlobStorage extends Closable {
  def saveBatch(blobs: Iterable[(BlobId, Blob)]): RollbackCommand;

  def loadBatch(ids: Iterable[BlobId]): Iterable[Option[Blob]];

  def deleteBatch(ids: Iterable[BlobId]): RollbackCommand;
}

trait Closable {
  def initialize(storeDir: File, blobIdFactory: BlobIdFactory, conf: Configuration): Unit;

  def disconnect(): Unit;
}