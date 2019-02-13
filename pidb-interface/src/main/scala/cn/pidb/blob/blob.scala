package cn.pidb.blob

import java.io._

import org.apache.commons.io.IOUtils

trait InputStreamSource {
  /**
    * note close input stream after consuming
    */
  def offerStream[T](consume: (InputStream) => T): T;
}

trait Blob extends Comparable[Blob] {
  val streamSource: InputStreamSource;
  val length: Long;
  val mimeType: MimeType;

  def offerStream[T](consume: (InputStream) => T): T = streamSource.offerStream(consume);

  def toBytes() = offerStream(IOUtils.toByteArray(_));

  override def toString = s"blob(length=${length},mime-type=${mimeType.text})";

  def makeTempFile(): File = {
    offerStream((is) => {
      val f = File.createTempFile("blob-", ".bin");
      IOUtils.copy(is, new FileOutputStream(f));
      f;
    })
  }

  override def compareTo(o: Blob) = this.length.compareTo(o.length);
}

trait StoredBlob extends Blob {
  val storeId: BlobId;
}

trait BlobId {
  def asByteArray(): Array[Byte];

  def asLiteralString(): String;
}

trait BlobIdFactory {
  def create(): BlobId;

  def readFromStream(is: InputStream): BlobId;

  def fromBytes(bytes: Array[Byte]): BlobId;

  def fromLiteralString(bid: String): BlobId;
}

object Blob {

  class BlobImpl(val streamSource: InputStreamSource, val length: Long, val mimeType: MimeType) extends Blob {
  }

  def withStoreId(blob: Blob, bid: BlobId): StoredBlob = {
    new StoredBlob() {
      override val storeId: BlobId = bid
      override val length: Long = blob.length
      override val streamSource: InputStreamSource = blob.streamSource
      override val mimeType: MimeType = blob.mimeType
    }
  }

  def fromBytes(bytes: Array[Byte]): Blob = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new ByteArrayInputStream(bytes);
        val t = consume(fis);
        fis.close();
        t;
      }
    }, 0, Some(MimeType.fromText("application/octet-stream")));
  }

  val EMPTY: Blob = fromBytes(Array[Byte]());

  def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None) = {
    new BlobImpl(iss,
      length,
      mimeType.getOrElse(MimeType.guessMimeType(iss)));
  }

  def fromFile(file: File, mimeType: Option[MimeType] = None): BlobImpl = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new FileInputStream(file);
        val t = consume(fis);
        fis.close();
        t;
      }
    },
      file.length(),
      mimeType);
  }
}