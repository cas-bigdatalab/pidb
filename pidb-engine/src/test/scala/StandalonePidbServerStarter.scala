import java.io.File

import cn.pidb.engine.PidbConnector
import org.apache.commons.io.FileUtils

object StandalonePidbServerStarter {
  def main(args: Array[String]) {
    //new TestBase().setupNewDatabase();
    //setting working dir to ./pidb-engine?
    //FileUtils.deleteDirectory(new File("./testdb"));
    PidbConnector.startServer(new File("./testdb"), new File("./neo4j.conf"));
  }
}