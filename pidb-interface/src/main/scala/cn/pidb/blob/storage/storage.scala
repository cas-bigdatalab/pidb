package cn.pidb.blob.storage

import java.io.{File, FileInputStream, FileOutputStream, InputStream}

import cn.pidb.blob._
import cn.pidb.util.ConfigEx._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Config, Logging}
import org.apache.commons.io.IOUtils

trait BlobStorage extends Closable {
  def save(bid: BlobId, blob: Blob);

  def load(bid: BlobId): InputStreamSource;

  def delete(bid: BlobId);
}

trait Closable {
  def initialize(storeDir: File, blobIdFactory: BlobIdFactory, conf: Config): Unit;

  def disconnect(): Unit;
}