import java.io.File

import cn.pidb.engine.PidbConnector
import org.apache.commons.io.FileUtils

object StandalonePidbServerStarter {
  def main(args: Array[String]) {
    //new TestBase().setupNewDatabase();
    //why current user dir is not ./pidb-engine?
    FileUtils.deleteDirectory(new File("./pidb-engine/testdb"));
    PidbConnector.startServer(new File("./pidb-engine/testdb"), new File("./pidb-engine/neo4j.conf"));
  }
}