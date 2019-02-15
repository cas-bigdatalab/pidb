package cn.aipm.image

import cn.pidb.blob._
import cn.aipm.service.Services


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

  override def argumentTypes(): (ValueType, ValueType) =
    ValueType.mimeType("image") -> ValueType.mimeType("image")
}