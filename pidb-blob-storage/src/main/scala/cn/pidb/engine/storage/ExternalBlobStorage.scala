package cn.pidb.engine.storage

import java.io._

import cn.pidb.blob._
import cn.pidb.blob.storage.{BlobStorage, RollbackCommand}
import cn.pidb.engine.buffer.Buffer
import cn.pidb.engine.util.{FileUtils, HBaseUtils}
import cn.pidb.util.ConfigurationUtils._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Configuration, Logging}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.concurrent.forkjoin.ForkJoinPool


trait Bufferable {

  def getAllBlob: Iterable[BlobId]

  def loadBlobBatch(BlobIds: Iterable[BlobId]): Iterable[Blob]

}

//FIXME: choose a better class name, this class is designed only for blob storage, not for nodes/properties
trait ExternalBlobStorage extends BlobStorage with Logging {
  protected var _blobIdFac: BlobIdFactory = _

  def getIdFac: BlobIdFactory = _blobIdFac

  def check(bid: BlobId): Boolean

  def delete(bid: BlobId): RollbackCommand;

  def save(bid: BlobId, blob: Blob): RollbackCommand;

  def load(bid: BlobId): Option[Blob];

  def createRollbackCommand(commands: Iterable[RollbackCommand]): RollbackCommand =
    new RollbackCommand() {
      override def perform(): Unit = commands.foreach(_.perform())
    }

  def deleteBatch(bids: Iterable[BlobId]): RollbackCommand = createRollbackCommand(bids.map(delete))

  def saveBatch(blobs: Iterable[(BlobId, Blob)]): RollbackCommand = createRollbackCommand(blobs.map(a => save(a._1, a._2)))

  def checkExistBatch(bids: Iterable[BlobId]): Iterable[Boolean] = bids.map(check)

  def loadBatch(bids: Iterable[BlobId]): Iterable[Option[Blob]] = bids.map(load)
}

// TODO ref
// TODO externalStorage?
class HybridBlobStorage(persistStorage: ExternalBlobStorage, val buffer: Buffer) extends ExternalBlobStorage {

  override def deleteBatch(bids: Iterable[BlobId]): RollbackCommand = {
    buffer.getBufferableStorage.deleteBatch(bids)
    persistStorage.deleteBatch(bids)
  }

  override def saveBatch(blobs: Iterable[(BlobId, Blob)]): RollbackCommand = buffer.getBufferableStorage.saveBatch(blobs)

  override def checkExistBatch(bids: Iterable[BlobId]): Iterable[Boolean] = bids.map(f =>
    buffer.getBufferableStorage.check(f) || persistStorage.check(f))

  override def loadBatch(bids: Iterable[BlobId]): Iterable[Option[Blob]] = persistStorage.loadBatch(bids)

  override def initialize(storeDir: File, blobIdFac: BlobIdFactory, conf: Configuration): Unit = {
    buffer.checkInit()
    buffer.initialize(storeDir, blobIdFac, conf)
  }

  override def disconnect(): Unit = {
    buffer.disconnect()
    persistStorage.disconnect()
  }

  override def save(bid: BlobId, blob: Blob): RollbackCommand = buffer.getBufferableStorage.save(bid, blob)

  //FIXME: what if it exist in buffer?
  override def load(bid: BlobId): Option[Blob] = persistStorage.load(bid)

  override def check(bid: BlobId): Boolean = buffer.getBufferableStorage.check(bid) || persistStorage.check(bid)

  override def delete(bid: BlobId): RollbackCommand = {
    buffer.getBufferableStorage.delete(bid)
    persistStorage.delete(bid)
  }
}

class HBaseBlobStorage extends ExternalBlobStorage {
  private var _table: Table = _
  //FIXME: _conn? <- _table means this object can be created lightly
  private var conn: Connection = _ //conn means don't create this object once more, it's time-consuming

  override def delete(bid: BlobId): RollbackCommand = {
    _table.delete(HBaseUtils.buildDelete(bid))
    new RollbackCommand {
      override def perform(): Unit = {
        //TODO
      }
    }
  }

  override def deleteBatch(bids: Iterable[BlobId]): RollbackCommand = {
    _table.delete(bids.map(f => HBaseUtils.buildDelete(f)).toList)

    new RollbackCommand {
      override def perform(): Unit = {
        //TODO
      }
    }
  }

  override def initialize(storeDir: File, blobIdFac: BlobIdFactory, conf: Configuration): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val zkQ = conf.getValueAsString("blob.storage.hbase.zookeeper.quorum", "localhost")
    val zkNode = conf.getValueAsString("blog.storage.hbase.zookeeper.znode.parent", "/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", zkQ)
    hbaseConf.set("zookeeper.znode.parent", zkNode)
    HBaseAdmin.available(hbaseConf)
    logger.info("successfully initial the connection to the zookeeper")
    //no db in hbase
    //val dbName = conf.getValueAsString("blob.storage.hbase.dbName", "testdb")
    //TODO: storageTable --> BLOB_TABLE? using specific names, instead of common-purposed names
    val tableName = conf.getValueAsString("blob.storage.hbase.tableName", "storageTable")
    val name = TableName.valueOf(tableName)
    conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    if (!admin.tableExists(name)) {
      //create it? prints helpful msg
      //logger.info("table not exists. create it in hbase")
      admin.createTable(new ModifyableTableDescriptor(name)
        .setColumnFamily(new ModifyableColumnFamilyDescriptor(HBaseUtils.columnFamily)))

      logger.info(s"table created: $tableName")
    }
    _table = conn.getTable(name, ForkJoinPool.commonPool())
    this._blobIdFac = blobIdFac
  }

  override def disconnect(): Unit = {
    _table.close()
    conn.close()
  }

  override def save(bid: BlobId, blob: Blob): RollbackCommand = {
    _table.put(HBaseUtils.buildPut(blob, bid))

    new RollbackCommand {
      override def perform(): Unit = {
        //TODO
      }
    }
  }

  override def load(bid: BlobId): Option[Blob] = {
    val res = _table.get(HBaseUtils.buildGetBlob(bid))
    if (!res.isEmpty) {
      val len = Bytes.toLong(res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyLen))
      val mimeType = Bytes.toLong(res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyMT))
      val value = res.getValue(HBaseUtils.columnFamily, HBaseUtils.qualifyFamilyBlob)
      val in = new ByteArrayInputStream(value)
      Some(Blob.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: InputStream => T): T = {
          val t = consume(in)
          in.close()
          t
        }
      }, len, Some(MimeType.fromCode(mimeType))))
    }
    else None
  }

  override def check(bid: BlobId): Boolean = !_table.exists(HBaseUtils.buildGetBlob(bid))
}

class FileBlobStorage extends ExternalBlobStorage with Bufferable {
  var _blobDir: File = _

  override def initialize(storeDir: File, blobIdFac: BlobIdFactory, conf: Configuration): Unit = {
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

  override def delete(bid: BlobId): RollbackCommand = {
    val f = FileUtils.blobFile(bid, _blobDir)
    if (f.exists()) f.delete()

    new RollbackCommand {
      override def perform(): Unit = {
        //TODO
      }
    }
  }

  override def getAllBlob: Iterable[BlobId] = FileUtils.listAllFiles(_blobDir).filter(f => f.isFile).map(f => _blobIdFac.fromLiteralString(f.getName))

  override def loadBlobBatch(bids: Iterable[BlobId]): Iterable[Blob] = {
    bids.map { bid =>
      FileUtils.readFromBlobFile(FileUtils.blobFile(bid, _blobDir), _blobIdFac)._2
    }
  }

  override def save(bid: BlobId, blob: Blob): RollbackCommand = {
    val file = FileUtils.blobFile(bid, _blobDir)
    file.getParentFile.mkdirs()

    val fos = new FileOutputStream(file)
    fos.write(bid.asByteArray())
    fos.writeLong(blob.mimeType.code)
    fos.writeLong(blob.length)

    blob.offerStream { bis =>
      IOUtils.copy(bis, fos);
    }
    fos.close()

    new RollbackCommand {
      override def perform(): Unit = {
        //TODO
      }
    }
  }

  //FIXME: consider that it does not exist
  override def load(bid: BlobId): Option[Blob] = Some(FileUtils.readFromBlobFile(FileUtils.blobFile(bid, _blobDir), _blobIdFac)._2)

  override def check(bid: BlobId): Boolean = FileUtils.blobFile(bid, _blobDir).exists()
}