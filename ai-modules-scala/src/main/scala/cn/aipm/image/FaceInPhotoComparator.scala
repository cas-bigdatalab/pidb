package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.{Config, ConfigEx}
import cn.pidb.util.ConfigEx._

class FaceInPhotoComparator extends SetComparator {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  def contains( photoBlob: Any,faceBlob: Any): Array[Array[Double]] = {
    null
  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }
}
