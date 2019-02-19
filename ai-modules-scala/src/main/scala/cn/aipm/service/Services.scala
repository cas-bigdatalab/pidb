package cn.aipm.service

import java.io.InputStream
import java.util.Properties
import java.io.BufferedInputStream
import java.io.FileInputStream
import scala.collection.mutable.HashMap

import scala.collection.immutable.Map
import scala.util.parsing.json.JSON


object Services {

  val configures = {
    val filePath =System.getProperty("user.dir") + "/ai-services.conf"
    val prop = new Properties()
    val configures = HashMap[String,String]()
    try {
      val in = new BufferedInputStream(new FileInputStream(filePath))
      prop.load(in) ///加载属性列表
      val properties = prop.propertyNames()
      while(properties.hasMoreElements){
        val k = properties.nextElement().toString
        configures.put(k, prop.getProperty(k))
      }
      in.close()
    } catch {
      case e: Exception =>
        System.out.println(e)
    }
    configures
  }

  def computeFaceSimilarity(img1InputStream:InputStream,img2InputStream:InputStream): Double={
    val serviceUrl = configures("FaceSimilarityServiceUrl")

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
    val serviceUrl = configures("PlateNumberServiceUrl")

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
    val serviceUrl = configures("AnimalClassifyServiceUrl")

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
