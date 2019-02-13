import java.io.File

import cn.pidb.blob.Blob
import cn.pidb.engine.PidbConnector
import org.apache.commons.io.FileUtils

object TestMain {
  def main(args: Array[String]): Unit = {
    val dir = "target/tmp"
    val n: Int = 10000

    println(s"dir: $dir, number: $n")
    FileUtils.deleteDirectory(new File(dir))
    val db = PidbConnector.openDatabase(new File(dir), new File("neo4j-ref.conf"));

    println("start inserting blobs...")
    val start = System.currentTimeMillis()

    for (i <- 0 to n) {
      val tx = db.beginTx()
      for (j <- 0 to 10000) {
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
    println(elapse * 0.0001 / n)
    db.shutdown()
  }
}
