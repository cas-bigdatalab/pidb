import java.io.File

import cn.pidb.blob.Blob
import cn.pidb.blob.Blob.BlobImpl
import cn.pidb.engine.PidbConnector
import org.junit.{After, Assert, Before, Test}
import org.neo4j.graphdb.GraphDatabaseService

//TOOD: use pidb-engine-test repository
class BlobHbaseTest {
  val confPath = "neo4j-hbase.conf"
  val dbPath = "target"
  var db: GraphDatabaseService = _
  var blob: BlobImpl = _
  var blob2: BlobImpl = _

  @Before
  def openDatabase_(): Unit = {
    db = PidbConnector.openDatabase(new File(dbPath + "/graph.db"),
      new File(confPath));
    blob = Blob.fromFile(new File("test.png"))
    blob2 = Blob.fromFile(new File("test1.png"))
  }

  @Test
  def testInsert(): Unit = {
    val tx = db.beginTx()
    val node = db.createNode()
    node.setProperty("name", "tom")
    node.setProperty("age", 29)
    node.setProperty("id", 1)
    //with a blob property
    node.setProperty("photo", blob)
    tx.success()
    tx.close()
    val tx_ = db.beginTx()
    //create a node
    val node1 = db.createNode()
    node1.setProperty("name", "bob")
    node1.setProperty("age", 30)
    node1.setProperty("photo", blob2)
    tx_.success()
    tx_.close()

  }

  @After
  def testGet(): Unit = {
    val r1 = db.execute("match (n) where n.name='bob' return n.name,n.age,n.photo").next()
    Assert.assertEquals("bob", r1.get("n.name"))
    Assert.assertEquals(30, r1.get("n.age"))
    Assert.assertArrayEquals(blob2.toBytes(), r1.get("n.photo").asInstanceOf[Blob].toBytes())
    val r2 = db.execute("match (n) where n.name='tom' return n.name,n.age,n.photo").next()
    Assert.assertEquals("tom", r2.get("n.name"))
    Assert.assertEquals(29, r2.get("n.age"))
    Assert.assertArrayEquals(blob.toBytes(), r2.get("n.photo").asInstanceOf[Blob].toBytes())


    db.execute("MATCH (a {name: \"bob\"}) DELETE a")
    db.execute("MATCH (a {name: \"tom\"}) DELETE a")

  }
}
