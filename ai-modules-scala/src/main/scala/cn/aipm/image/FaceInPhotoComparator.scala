package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.{Config, ConfigEx}
import cn.pidb.util.ConfigEx._

class FaceInPhotoComparator extends SetComparator {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  def contains( photoBlob: Any,faceBlob: Any): Boolean = {
    faceBlob.asInstanceOf[Blob].offerStream(is1=>{
      photoBlob.asInstanceOf[Blob].offerStream(is2=>{
         Services.initialize(aipmHttpHostUrl).isFaceInPhoto(is1,is2)
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = ConfigEx.config2Ex(conf).getRequiredValueAsString("aipm.http.host.url")
  }
}
