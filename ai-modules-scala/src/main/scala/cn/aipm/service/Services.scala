package cn.aipm.service

import scala.collection.immutable.Map
import java.io.InputStream
import scala.util.parsing.json.JSON


object Services {
  var hostUrl = "http://10.0.86.128:8081/service/"

  def computeFaceSimilarity(img1InputStream:InputStream,img2InputStream:InputStream): Double={
    val serviceUrl = hostUrl + "face/similarity/"

    val contents = Map("image1" -> img1InputStream, "image2" -> img2InputStream)

    val res = WebUtils.doPost(serviceUrl,inStreamContents = contents)
    val json:Option[Any] = JSON.parseFull(res)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    if(map("res").asInstanceOf[Boolean]){
      map("value").asInstanceOf[Double]
    }
    else{
      -1
    }

  }

  def extractPlateNumber(img1InputStream:InputStream): String= {
    val serviceUrl = hostUrl + "plate/"

    val contents = Map("image1" -> img1InputStream)

    val res = WebUtils.doPost(serviceUrl, inStreamContents = contents)
    val json: Option[Any] = JSON.parseFull(res)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    if (map("res").asInstanceOf[Boolean]) {
      map("value").asInstanceOf[String]
    }
    else {
      ""
    }
  }

}
