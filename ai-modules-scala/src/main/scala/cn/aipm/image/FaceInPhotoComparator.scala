package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.Config
import cn.pidb.util.ConfigEx._

class FaceInPhotoComparator extends ValueComparator{
  def compare(faceBlob: Any, photoBlob: Any): Double = {
    faceBlob.asInstanceOf[Blob].offerStream(is1=>{
      photoBlob.asInstanceOf[Blob].offerStream(is2=>{
         if (Services.isFaceInPhoto(is1,is2)) 1 else 0
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    //val aipmUrl = conf.getRequiredValueAsString("aipm-http-url")
    //...
  }
}
