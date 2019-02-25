package cn.pidb.engine.storage

import java.io._

import cn.pidb.blob._
import cn.pidb.blob.storage.BlobStorage
import cn.pidb.engine.buffer.Buffer
import cn.pidb.engine.util.{FileUtils, HBaseUtils}
import cn.pidb.util.ConfigEx._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Config, Logging}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.concurrent.forkjoin.ForkJoinPool


trait Bufferable {

  def getAllBlob : Iterable[BlobId]

  def loadBlobBatch (BlobIds : Iterable[BlobId]) : Iterable[Blob]

}

trait Storage extends BlobStorage with Logging {
  protected var _blobIdFac : BlobIdFactory = _
  def getIdFac : BlobIdFactory = _blobIdFac
  def check(bid : BlobId) : Boolean

  def deleteBatch(bids: Iterable[BlobId]) : Unit = bids.foreach(delete)

  def saveBatch(blobs: Iterable[(BlobId, Blob)]): Unit = blobs.foreach(a => save(a._1, a._2))

  def checkExistBatch(bids: Iterable[BlobId]): Iterable[Boolean] = bids.map(check)

  def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource] = bids.map(load)
}
// TODO ref
class HybridStorage(persistStorage : Storage, val buffer : Buffer) extends Storage {

  override def deleteBatch(bids: Iterable[BlobId]): Unit = {
    buffer.getBufferableStorage.deleteBatch(bids)
    persistStorage.deleteBatch(bids)
  }

  override def saveBatch(blobs: Iterable[(BlobId, Blob)]): Unit = buffer.getBufferableStorage.saveBatch(blobs)

  override def checkExistBatch(bids: Iterable[BlobId]): Iterable[Boolean] = bids.map(f =>
    buffer.getBufferableStorage.check(f) || persistStorage.check(f))

  override def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource] = persistStorage.loadBatch(bids)

  override def initialize(storeDir: File, blobIdFac : BlobIdFactory, conf: Config): Unit = {
    buffer.checkInit()
    buffer.initialize(storeDir,blobIdFac ,conf)
  }

  override def disconnect(): Unit = {
    buffer.disconnect()
    persistStorage.disconnect()
  }

  override def save(bid: BlobId, blob: Blob): Unit = buffer.getBufferableStorage.save(bid, blob)

  override def load(bid: BlobId): InputStreamSource =  persistStorage.load(bid)

  override def check(bid: BlobId): Boolean =  buffer.getBufferableStorage.check(bid) || persistStorage.check(bid)

  override def delete(bid: BlobId): Unit =  {
    buffer.getBufferableStorage.delete(bid)
    persistStorage.delete(bid)
  }
}

class HBaseStorage extends Storage {
  private var _table: Table = _
  private var conn: Connection = _

  override def delete(bid : BlobId) : Unit =
    _table.delete(HBaseUtils.buildDelete(bid))

  override def deleteBatch(bids: Iterable[BlobId]): Unit =
    _table.delete(bids.map(f => HBaseUtils.buildDelete(f)).toList)

  override def initialize(storeDir: File, blobIdFac : BlobIdFactory, conf: Config): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val zkQ = conf.getValueAsString("blob.storage.hbase.zookeeper.quorum", "localhost")
    val zkNode = conf.getValueAsString("blog.storage.hbase.zookeeper.znode.parent", "/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", zkQ)
    hbaseConf.set("zookeeper.znode.parent", zkNode)
    HBaseAdmin.available(hbaseConf)
    logger.info("successfully initial the connection to the zookeeper")
    val dbName = conf.getValueAsString("blob.storage.hbase.dbName", "testdb")
    val tableName = conf.getValueAsString("blob.storage.hbase.tableName", "storageTable")
    val name = TableName.valueOf(dbName + "." + tableName)
    conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    if (!admin.tableExists(name)) {
      logger.info("table not exists. create it in hbase")
      admin.createTable(new ModifyableTableDescriptor(TableName.valueOf(dbName + "." + tableName))
        .setColumnFamily(new ModifyableColumnFamilyDescriptor(HBaseUtils.columnFamily)))
    }
    _table = conn.getTable(name, ForkJoinPool.commonPool())
    this._blobIdFac = blobIdFac
  }

  override def disconnect(): Unit = {
    _table.close()
    conn.close()
  }

  override def save(bid: BlobId, blob: Blob): Unit =
    _table.put(HBaseUtils.buildPut(blob, bid))

  override def load(bid: BlobId): InputStreamSource = {
    val res = _table.get(HBaseUtils.buildGetBlob(bid))
    if (!res.isEmpty) {
      val len = Bytes.toLong(res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyLen))
      val mimeType = Bytes.toLong(res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyMT))
      val value = res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyBlob)
      val in = new ByteArrayInputStream(value)
      Blob.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: InputStream => T): T = {
          val t = consume(in)
          in.close()
          t
        }
      }, len, Some(MimeType.fromCode(mimeType))).streamSource
    }
    else null
  }

  override def check(bid: BlobId): Boolean = !_table.exists(HBaseUtils.buildGetBlob(bid))
}

class FileStorage extends Storage with Bufferable {
  var _blobDir : File = _

  override def initialize(storeDir: File, blobIdFac : BlobIdFactory, conf: Config): Unit = {
    val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
    _blobDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"))
    if (!_blobDir.exists()) {
      _blobDir.mkdirs()
    }
    _blobIdFac = blobIdFac
    logger.info(s"using storage dir: ${_blobDir.getCanonicalPath}")
  }

  override def disconnect(): Unit = {
  }

  override def delete (bid : BlobId) : Unit = {
    val f =FileUtils.blobFile(bid, _blobDir)
    if (f.exists()) f.delete()
  }

  override def getAllBlob: Iterable[BlobId] = FileUtils.listAllFiles(_blobDir).filter(f => f.isFile).map(f => _blobIdFac.fromLiteralString(f.getName))

  override def loadBlobBatch(bids: Iterable[BlobId]): Iterable[Blob] = {
    bids.map { bid =>
      FileUtils.readFromBlobFile(FileUtils.blobFile(bid , _blobDir), _blobIdFac)._2
    }
  }

  override def save(bid: BlobId, blob: Blob): Unit = {
    val file = FileUtils.blobFile(bid, _blobDir)
    file.getParentFile.mkdirs()

    val fos = new FileOutputStream(file)
    fos.write(bid.asByteArray)
    fos.writeLong(blob.mimeType.code)
    fos.writeLong(blob.length)

    blob.offerStream { bis =>
      IOUtils.copy(bis, fos);
    }
    fos.close()
  }

  override def load(bid: BlobId): InputStreamSource = FileUtils.readFromBlobFile(FileUtils.blobFile(bid, _blobDir), _blobIdFac)._2.streamSource

  override def check(bid: BlobId): Boolean = FileUtils.blobFile(bid, _blobDir).exists()
}