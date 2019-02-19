package cn.aipm.service

import java.io.InputStream
import java.util.Properties
import java.io.BufferedInputStream
import java.io.FileInputStream
import scala.collection.mutable.HashMap

import scala.collection.immutable.Map
import scala.util.parsing.json.JSON


class Services(private val _aipmHttpHostUrl:String) {
  val servicesPath = Map(
    "FaceSim"-> "service/face/similarity/",
    "FaceInPhoto"-> "service/face/in_photo/",
    "PlateNumber"-> "service/plate/",
    "ClassifyAnimal"-> "service/classify/dogorcat/"
  )
  def getServiceUrl(name:String):String = {
    if (_aipmHttpHostUrl.endsWith("/")){
      _aipmHttpHostUrl + servicesPath(name)
    }
    else{
      _aipmHttpHostUrl + "/" + servicesPath(name)
    }
  }

  def computeFaceSimilarity(img1InputStream:InputStream,img2InputStream:InputStream): Double={
    val serviceUrl = getServiceUrl("FaceSim")
    print(serviceUrl)

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


  def isFaceInPhoto(faceImgInputStream:InputStream,photoImgInputStream:InputStream): Boolean={
    val serviceUrl = getServiceUrl("FaceInPhoto")

    val contents = Map("image1" -> faceImgInputStream, "image2" -> photoImgInputStream)

    val res = WebUtils.doPost(serviceUrl,inStreamContents = contents)
    val json:Option[Any] = JSON.parseFull(res)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    if(map("res").asInstanceOf[Boolean]){
      map("value").asInstanceOf[Boolean]
    }
    else{
      false
    }

  }


  def extractPlateNumber(img1InputStream:InputStream): String= {
    val serviceUrl = getServiceUrl("PlateNumber")

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

  def classifyAnimal(img1InputStream:InputStream): String= {
    val serviceUrl = getServiceUrl("ClassifyAnimal")

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

object Services{
  def initialize(aipmHttpHostUrl:String):Services = new Services(aipmHttpHostUrl)
}
