package cn.aipm.test
import cn.pidb.util.Config

class ConfigTemp extends Config{
  override def getRaw(name: String): Option[String] = {
    val configs = Map("aipmHttpHostUrl"->"http://10.0.86.128:8081/")
    configs.get(name)
  }
}

class TestBase {
  val config = new ConfigTemp()
}
