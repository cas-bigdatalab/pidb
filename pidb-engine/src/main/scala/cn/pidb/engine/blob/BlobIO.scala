package cn.pidb.engine.blob

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import cn.pidb.blob._
import cn.pidb.blob.storage.BlobStorage
import cn.pidb.engine.blob.extensions._
import cn.pidb.engine.{BlobCacheInSession, BlobPropertyStoreService, ThreadBoundContext}
import cn.pidb.util.ReflectUtils._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Logging, StreamUtils}
import org.neo4j.driver.internal.packstream.PackStream
import org.neo4j.driver.internal.types.{TypeConstructor, TypeRepresentation}
import org.neo4j.driver.internal.value.ValueAdapter
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.types.Type
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.store.PropertyType
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy
import org.neo4j.kernel.impl.transaction.state.{PropertyDeleter, RecordAccess}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobIO extends Logging {
  val BOLT_VALUE_TYPE_BLOB_INLINE = PackStream.RESERVED_C5;
  val BOLT_VALUE_TYPE_BLOB_REMOTE = PackStream.RESERVED_C4;
  val MAX_INLINE_BLOB_BYTES = 10240;

  val blobIdFactory = BlobIdFactory.get
  //10k

  def decodeBlob(bytes: Array[Byte], conf: Config): Blob = {
    val baos = new ByteArrayOutputStream();
    baos.write(bytes);
    val bais = new ByteArrayInputStream(baos.toByteArray);
    val longs = (0 to 3).map(x => bais.readLong()).toArray;
    _readBlobValue(longs, conf).blob;
  }

  def encodeBlob(blob: Blob, trsa: TransactionRecordStateAware): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val blobId = blobIdFactory.create();
    _wrapBlobEntryAsLongArray(blob, blobId, 0).foreach(baos.writeLong(_));

    //trsa.blobPropertyStoreService.blobStorage.saveBatch(Array(blobId -> blob))
    trsa.recordState.asInstanceOf[TransactionRecordStateExtension]
      .committedBlobBuffer.prepare2AddBlob(blobId, blob);

    baos.toByteArray;
  }

  /**
    * common interface for org.neo4j.driver.internal.packstream.PackOutput & org.neo4j.bolt.v1.packstream.PackOutput
    */
  trait PackOutputInterface {
    def writeByte(b: Byte);

    def writeLong(l: Long);

    def writeInt(i: Int);

    def writeBytes(bs: Array[Byte]);
  }

  private def _writeBlobIntoBoltStream(blob: Blob, out: PackOutputInterface, useInlineAlways: Boolean): Unit = {
    //create a temp blodid
    val tempBlobId = blobIdFactory.create();
    val inline = useInlineAlways || (blob.length <= MAX_INLINE_BLOB_BYTES);
    //write marker
    out.writeByte(if (inline) {
      BOLT_VALUE_TYPE_BLOB_INLINE
    }
    else {
      BOLT_VALUE_TYPE_BLOB_REMOTE
    });

    //write blob entry
    _wrapBlobEntryAsLongArray(blob, tempBlobId).foreach(out.writeLong(_));

    //write inline
    if (inline) {
      val bytes = blob.toBytes();
      out.writeBytes(bytes);
    }
    else {
      //write as a HTTP resource
      val config: Config = ThreadBoundContext.config;
      val httpConnectorUrl: String = config.asInstanceOf[RuntimeContext].contextGet("blob.server.connector.url").asInstanceOf[String];
      val bpss = config.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
      //http://localhost:1224/blob
      val bs = httpConnectorUrl.getBytes("utf-8");
      out.writeInt(bs.length);
      out.writeBytes(bs);
      config.asInstanceOf[RuntimeContext].contextGet[BlobCacheInSession].put(tempBlobId, blob);
      ThreadBoundContext.recordState.asInstanceOf[TransactionRecordStateExtension].cachedStreams.add(tempBlobId);
    }
  }

  def writeBlob(blob: Blob, packer: PackStream.Packer): Unit = {
    writeBlob(blob, packer._get("out").asInstanceOf[org.neo4j.driver.internal.packstream.PackOutput])
  }

  def writeBlob(blob: Blob, out: org.neo4j.driver.internal.packstream.PackOutput): Unit = {
    _writeBlobIntoBoltStream(blob, new PackOutputInterface() {
      override def writeByte(b: Byte): Unit = out.writeByte(b);

      override def writeInt(i: Int): Unit = out.writeInt(i);

      override def writeBytes(bs: Array[Byte]): Unit = out.writeBytes(bs);

      override def writeLong(l: Long): Unit = out.writeLong(l);
    }, true);
  }

  def writeBlob(blob: Blob, out: org.neo4j.bolt.v1.packstream.PackOutput): Unit = {
    _writeBlobIntoBoltStream(blob, new PackOutputInterface() {
      override def writeByte(b: Byte): Unit = out.writeByte(b);

      override def writeInt(i: Int): Unit = out.writeInt(i);

      override def writeBytes(bs: Array[Byte]): Unit = out.writeBytes(bs, 0, bs.length);

      override def writeLong(l: Long): Unit = out.writeLong(l);
    }, false);
  }

  trait PackInputInterface {
    def peekByte(): Byte;

    def readByte(): Byte;

    def readBytes(bytes: Array[Byte], offset: Int, toRead: Int);

    def readInt(): Int;

    def readLong(): Long;
  }

  private def _readBlobFromBoltStreamIfAvailable(in: PackInputInterface): Blob = {
    val byte = in.peekByte();
    byte match {
      case BOLT_VALUE_TYPE_BLOB_REMOTE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val (bid, length, mt) = _unpackBlob(values.toArray);

        val lengthUrl = in.readInt();
        val bs = new Array[Byte](lengthUrl);
        in.readBytes(bs, 0, lengthUrl);

        val url = new String(bs, "utf-8");
        new RemoteBlob(url, bid, length, mt);

      case BOLT_VALUE_TYPE_BLOB_INLINE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val (_, length, mt) = _unpackBlob(values.toArray);

        //read inline
        val bs = new Array[Byte](length.toInt);
        in.readBytes(bs, 0, length.toInt);
        new InlineBlob(bs, length, mt);

      case _ => null;
    }
  }

  def readBlobFromBoltStreamIfAvailable(unpacker: PackStream.Unpacker): Value = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.driver.internal.packstream.PackInput];
    val blob = _readBlobFromBoltStreamIfAvailable(new PackInputInterface() {
      def peekByte(): Byte = in.peekByte();

      def readByte(): Byte = in.readByte();

      def readBytes(bytes: Array[Byte], offset: Int, toRead: Int) = in.readBytes(bytes, offset, toRead);

      def readInt(): Int = in.readInt();

      def readLong(): Long = in.readLong();
    });

    if (null == blob)
      null;
    else
      new BoltBlobValue(blob);
  }

  def readBlobFromBoltStreamIfAvailable(unpacker: org.neo4j.bolt.v1.packstream.PackStream.Unpacker): AnyValue = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.bolt.v1.packstream.PackInput];
    val blob = _readBlobFromBoltStreamIfAvailable(new PackInputInterface() {
      def peekByte(): Byte = in.peekByte();

      def readByte(): Byte = in.readByte();

      def readBytes(bytes: Array[Byte], offset: Int, toRead: Int) = in.readBytes(bytes, offset, toRead);

      def readInt(): Int = in.readInt();

      def readLong(): Long = in.readLong();
    });

    if (null == blob)
      null;
    else
      new BlobValue(blob);
  }

  private def _wrapBlobEntryAsLongArray(blob: Blob, blobId: BlobId, keyId: Int = 0): Array[Long] = {
    val values = new Array[Long](4);
    //val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [____,____][____,____][____,____][____,____][[____,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (t=type, k=keyId)
    v1: [llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)
    v2: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    v3: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    */
    values(0) = keyId | (PropertyType.BLOB.intValue() << 24);
    values(1) = blob.mimeType.code | (blob.length << 16);

    val la = StreamUtils.convertByteArray2LongArray(blobId.asByteArray());
    values(2) = la(0);
    values(3) = la(1);

    values;
  }

  private def _writeBlob(blob: Blob, blobId: BlobId, valueWriter: ValueWriter[_])(extraOp: => Unit) = {
    val keyId = valueWriter._get("keyId").asInstanceOf[Int];
    val block = valueWriter._get("block").asInstanceOf[PropertyBlock];

    extraOp;

    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)
    block.setValueBlocks(_wrapBlobEntryAsLongArray(blob, blobId, keyId));
  }

  def saveBlob(blob: Blob, valueWriter: ValueWriter[_]) = {
    val blobId = blobIdFactory.create();
    _writeBlob(blob, blobId, valueWriter) {
      valueWriter._get("stringAllocator").asInstanceOf[TransactionRecordStateAware]
        .recordState.asInstanceOf[TransactionRecordStateExtension].committedBlobBuffer.prepare2AddBlob(blobId, blob);
    };
  }

  def readBlobValue(values: Array[Long], cursor: DefaultPropertyCursor): BlobValue = {
    _readBlobValue(values, cursor._get("read.properties.configuration").asInstanceOf[Config]);
  }

  def _unpackBlob(values: Array[Long]): (BlobId, Long, MimeType) = {
    //val keyId = PropertyBlock.keyIndexId(values(0));
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val bid = blobIdFactory.fromLongArray(values(2), values(3));
    val mt = MimeType.fromCode(mimeType);
    (bid, length, mt);
  }

  def _readBlobValue(values: Array[Long], conf: Config): BlobValue = {
    val (bid, length, mt) = _unpackBlob(values);
    val storage: BlobStorage = conf.asInstanceOf[RuntimeContext].contextGet[BlobPropertyStoreService]().blobStorage;
    val loaded = storage.loadBatch(Array(bid)).head;
    val blob = Blob.fromInputStreamSource(loaded.get.streamSource, length, Some(mt));
    new BlobValue(Blob.withStoreId(blob, bid));
  }

  def readBlobValue(block: PropertyBlock, conf: Config): BlobValue = {
    _readBlobValue(block.getValueBlocks(), conf);
  }

  def prepare2DeleteBlobProperty(deleter: PropertyDeleter, primitiveProxy: RecordProxy[_, Void],
                                 propertyKey: Int,
                                 propertyRecords: RecordAccess[PropertyRecord, PrimitiveRecord],
                                 block: PropertyBlock): Unit = {
    val values = block.getValueBlocks;
    val length = values(1) >> 16;
    //val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));
    val state = deleter.asInstanceOf[TransactionRecordStateAware].recordState.asInstanceOf[TransactionRecordStateExtension];
    val conf = state._get("nodeStore.configuration").asInstanceOf[Config];
    val bid = blobIdFactory.fromLongArray(values(2), values(3));
    //soft delete
    state.committedBlobBuffer.prepare2DeleteBlob(bid);
  }
}

class BoltBlobValue(val blob: Blob)
  extends ValueAdapter with BlobHolder {

  val BOLT_BLOB_TYPE = new TypeRepresentation(TypeConstructor.BLOB);

  override def `type`(): Type = BOLT_BLOB_TYPE;

  override def equals(obj: Any): Boolean = obj.isInstanceOf[BoltBlobValue] &&
    obj.asInstanceOf[BoltBlobValue].blob.equals(this.blob);

  override def hashCode: Int = blob.hashCode()

  override def asBlob: Blob = blob;

  override def asObject = blob;

  override def toString: String = s"BoltBlobValue(blob=${blob.toString})"
}