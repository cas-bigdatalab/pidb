package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.{Config, ConfigEx}
import cn.pidb.util.ConfigEx._

class FaceInPhotoComparator extends ValueComparator{
  var aipmHttpHostUrl = "http://127.0.0.1/"

  def compare(faceBlob: Any, photoBlob: Any): Double = {
    faceBlob.asInstanceOf[Blob].offerStream(is1=>{
      photoBlob.asInstanceOf[Blob].offerStream(is2=>{
         if (Services.initialize(aipmHttpHostUrl).isFaceInPhoto(is1,is2)) 1 else 0
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = ConfigEx.config2Ex(conf).getRequiredValueAsString("aipmHttpHostUrl")
  }
}
