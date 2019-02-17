package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.Config
import cn.pidb.util.ConfigEx._


class FaceSimilarityComparator extends ValueComparator {
  def compare(blob1: Any, blob2: Any): Double = {
//    var is1:InputStream = null
//    var is2:InputStream = null
    blob1.asInstanceOf[Blob].offerStream(is1=>{

      blob2.asInstanceOf[Blob].offerStream(is2=>{

        Services.computeFaceSimilarity(is1,is2)
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    //val aipmUrl = conf.getRequiredValueAsString("aipm-http-url")
    //...
  }
}