package BatchFlushTest
import java.io.File

import cn.pidb.blob.Blob
import cn.pidb.engine.PidbConnector
import org.apache.commons.io.FileUtils

//capitalizes 1st letter
object putHbase {
  def main(args: Array[String]): Unit = {
    val dir = "target/tmp"
    val n: Int = 1

    println(s"dir: $dir, number: $n")
    FileUtils.deleteDirectory(new File(dir))
    val db = PidbConnector.openDatabase(new File(dir), new File("neo4j-hbase.conf"));

    println("start inserting blobs...")
    val start = System.currentTimeMillis()

    for (i <- 0 to n) {
      val tx = db.beginTx()
      for (j <- 0 to 1000) {
        val node = db.createNode()
        node.setProperty("id", j)
        //with a blob property
        node.setProperty("photo", Blob.fromFile(new File("test.png")));
      }
      tx.success()
      tx.close()
    }


    val end = System.currentTimeMillis()
    val elapse = end - start
    println(elapse)
    println(elapse * 0.001 / n)
    db.shutdown()
  }
}
