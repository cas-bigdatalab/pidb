import java.io.File

import cn.pidb.engine.PidbConnector

class LocalPidbWithHbaseTest extends LocalPidbTest {
  override def openDatabase() =
    PidbConnector.openDatabase(new File("./testdb/data/databases/graph.db"),
      new File("./neo4j-hbase.properties"));
}