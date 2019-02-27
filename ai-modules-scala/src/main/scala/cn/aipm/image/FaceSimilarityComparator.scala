package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services
import cn.pidb.util.Config
import cn.pidb.util.ConfigEx._


class FaceSimilarityComparator extends SetComparator {
  var aipmHttpHostUrl = "http://127.0.0.1/"

  def compare(blob1: Any, blob2: Any): Array[Array[Double]] = {
    blob1.asInstanceOf[Blob].offerStream(is1=>{
      blob2.asInstanceOf[Blob].offerStream(is2=>{
        val temp = Services.initialize(aipmHttpHostUrl).computeFaceSimilarity(is1,is2)
        if (temp != null){
          val arr:Array[Array[Double]] = new Array[Array[Double]](temp.size)
          var i:Int = 0
          for(t<-temp){
            arr(i) = t.toArray
            i += 1
          }
          arr
        }
        else{
          null
        }
      })

    })

  }

  override def initialize(conf: Config): Unit = {
    aipmHttpHostUrl = conf.getRequiredValueAsString("aipm.http.host.url")
  }
}