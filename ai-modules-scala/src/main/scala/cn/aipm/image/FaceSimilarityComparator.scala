package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.{Config, ConfigEx}
import cn.pidb.util.ConfigEx._


class FaceSimilarityComparator extends ValueComparator {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  def compare(blob1: Any, blob2: Any): Double = {
    blob1.asInstanceOf[Blob].offerStream(is1=>{
      blob2.asInstanceOf[Blob].offerStream(is2=>{
        Services.initialize(aipmHttpHostUrl).computeFaceSimilarity(is1,is2)
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = ConfigEx.config2Ex(conf).getRequiredValueAsString("aipmHttpHostUrl")
  }
}