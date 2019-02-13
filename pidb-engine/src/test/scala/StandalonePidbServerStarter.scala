import java.io.File

import cn.pidb.engine.PidbConnector

object StandalonePidbServerStarter {
  def main(args: Array[String]) {
    new TestBase().setupNewDatabase();
    PidbConnector.startServer(new File("./testdb"), new File("./neo4j.conf"));
  }
}