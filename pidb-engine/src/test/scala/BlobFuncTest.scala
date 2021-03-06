import java.io.{File, FileInputStream}
import java.util

import cn.pidb.blob.Blob
import cn.pidb.engine.PidbConnector
import cn.pidb.util.CodecUtils
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Test}

class BlobFuncTest {
  @Test
  def test2(): Unit = {
    FileUtils.deleteDirectory(new File("./testdb"));
    //create a new database
    val db = openDatabase();
    val tx = db.beginTx();

    Assert.assertEquals(0, db.execute("return Blob.empty() as blob").next().get("blob")
      .asInstanceOf[Blob].length);

    Assert.assertEquals(0, db.execute("return Blob.len(Blob.empty()) as len").next().get("len").asInstanceOf[Long]);
    Assert.assertArrayEquals("hello world".getBytes("utf-8"),
      db.execute("return Blob.fromUTF8String('hello world') as x").next().get("x").asInstanceOf[Blob].toBytes());
    Assert.assertEquals("hello world",
      db.execute("return Blob.toUTF8String(Blob.fromUTF8String('hello world')) as x").next().get("x").asInstanceOf[String]);

    //create a node
    val node1 = db.createNode();
    node1.setProperty("name", "bob");
    //with a blob property
    node1.setProperty("photo", Blob.fromFile(new File("./test.png")));
    db.execute("create (n: Person {name:'yahoo', photo: Blob.fromFile('./test2.jpg')})");

    val len2 = db.execute("return Blob.len(Blob.fromFile('./test.png')) as len").next().get("len").asInstanceOf[Long];
    Assert.assertEquals(len2, new File("./test.png").length());

    val len = db.execute("match (n) where n.name='bob' return Blob.len(n.photo) as len").next().get("len").asInstanceOf[Long];
    Assert.assertEquals(len, new File("./test.png").length());

    val result: util.Map[String, AnyRef] = db.execute(
      """
      match (n) where n.name='yahoo'
      return Blob.len(n.photo) as len,
      Blob.mime(n.photo) as mimetype,
      Blob.mime1(n.photo) as majormime,
      Blob.mime2(n.photo) as minormime
      """).next();

    Assert.assertEquals(new File("./test2.jpg").length(), result.get("len").asInstanceOf[Long]);
    Assert.assertEquals("image/jpeg", result.get("mimetype").asInstanceOf[String]);
    Assert.assertEquals("image", result.get("majormime").asInstanceOf[String]);
    Assert.assertEquals("jpeg", result.get("minormime").asInstanceOf[String]);
    val digestHex = CodecUtils.md5AsHex(new FileInputStream(new File("./test2.jpg")));

    Assert.assertEquals(1, db.execute("match (n) where Blob.mime(n.photo)='image/png' return n").stream().count());
    Assert.assertEquals(2, db.execute("match (n) where Blob.mime1(n.photo)='image' return n").stream().count());

    tx.success();
    tx.close();
    db.shutdown();
  }

  def openDatabase() = PidbConnector.openDatabase(new File("./testdb/data/databases/graph.db"),
    new File("./neo4j.conf"));
}