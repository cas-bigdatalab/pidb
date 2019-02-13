package cn.pidb.engine

import java.io.{ByteArrayInputStream, File, InputStream}

import cn.pidb.blob.{Blob, BlobId, InputStreamSource, MimeType}
import cn.pidb.engine.storage.BlobStorage
import cn.pidb.util.ConfigEx._
import cn.pidb.util.{Config, ConfigEx, Logging}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import cn.pidb.engine.util.HBaseUtils

import scala.collection.JavaConversions._

class HBaseBlobStorage extends BlobStorage with ExternalBlobStorage with Logging {

  private var _table: Table = _
  private var conn: Connection = _

  override def deleteBatch(bids: Iterable[BlobId]): Unit =
    _table.delete(bids
      .map(bid => HBaseUtils.buildDelete(bid)).toList)

  override def save(bid: BlobId, blob: Blob): Unit = {
    _table.put(HBaseUtils.buildPut(blob, bid))
  }

  override def saveBatch(blobs: Iterable[(BlobId, Blob)]): Unit = {
    _table.put(blobs.map(iter => HBaseUtils.buildPut(iter._2, iter._1)).toList)
  }

  override def containsBatch(bids: Iterable[BlobId]): Iterable[Boolean] = {
    bids.map { bid =>
      !_table.get(HBaseUtils.buildGetBlob(bid)).isEmpty
    }
  }

  override def initialize(storeDir: File, conf: Config): Unit = {
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
      admin.createTable(new HTableDescriptor(TableName.valueOf(dbName + "." + tableName))
        .addFamily(new HColumnDescriptor(HBaseUtils.columnFamily)))
    }
    _table = conn.getTable(name)

  }

  override def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource] = {
    bids.map { a: BlobId =>
      val res = _table.get(HBaseUtils.buildGetBlob(a))
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
  }

  override def disconnect(): Unit = {
    _table.close()
    conn.close()
  }

  override def load(bid: BlobId): InputStreamSource = loadBatch(Array(bid)).head

  override def delete(bid: BlobId): Unit = deleteBatch(Array(bid))
}