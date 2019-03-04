package cn.pidb.blob.storage

import java.io.File

import cn.pidb.blob._
import cn.pidb.util.Config

trait BlobStorage extends Closable {
  def saveBatch(blobs: Iterable[(BlobId, Blob)]);

  def loadBatch(ids: Iterable[BlobId]): Iterable[InputStreamSource];

  def deleteBatch(ids: Iterable[BlobId]);
}

trait Closable {
  def initialize(storeDir: File, blobIdFactory: BlobIdFactory, conf: Config): Unit;

  def disconnect(): Unit;
}