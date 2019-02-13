package cn.aipm.service

import scala.collection.immutable.Map
import java.io.InputStream
import scala.util.parsing.json.JSON


object Services {
  var localDebug = false  //用于本地测试
  var hostUrl = "http://10.0.86.128:8081/service/"
  def computeFaceSimilarity(filepath1:String,filepath2:String ): Double ={
    if(localDebug == true){
      return 0.5
    }
    val serviceUrl = hostUrl + "face/similarity/"

    val contents = Map("image1" -> filepath1, "image2" -> filepath2)

    val res = WebUtils.doPost(serviceUrl,strContents = contents)
    val json:Option[Any] = JSON.parseFull(res)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    if(map("res").asInstanceOf[Boolean]){
      map("value").asInstanceOf[Double]
    }
    else{
      -1
    }
  }
  def computeFaceSimilarity(img1InputStream:InputStream,img2InputStream:InputStream): Double={
    if(localDebug == true){
      return 0.5
    }
    val serviceUrl = hostUrl + "face/similarity/blob/"

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

}
