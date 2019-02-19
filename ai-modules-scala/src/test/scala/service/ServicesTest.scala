package cn.aipm.service

import java.io.{File, FileInputStream}

import org.junit.{Assert, Test}


class ServicesTest {
  val hostUrl = "http://10.0.86.128:8081/"

  @Test
  def test1(): Unit = {
    var image_path1 = "E:/[face]/unknown/test.jpg"
    var image_path2 = "E:/[face]/unknown/test2.jpg"
    val file1 = new File(image_path1)
    val file2 = new File(image_path2)
    val in1 = new FileInputStream(file1)
    val in2 = new FileInputStream(file2)
//    Services.hostUrl="http://127.0.0.1:8081/service/"
    val sim = Services.initialize(hostUrl).computeFaceSimilarity(in1, in2)
    print(sim)
  }


  @Test
  def test2(): Unit = {
    var image_path1 = "E:\\[pidb-ai-code]\\plate_number\\test1.jpg"
    val file1 = new File(image_path1)
    val in1 = new FileInputStream(file1)
    val plate = Services.initialize(hostUrl).extractPlateNumber(in1)
    print(plate)
  }

  @Test
  def test3(): Unit = {
    var image_path1 = "C:\\Users\\hai\\Desktop\\cat.1.jpg"
    val file1 = new File(image_path1)
    val in1 = new FileInputStream(file1)
    val animal = Services.initialize(hostUrl).classifyAnimal(in1)
    print(animal)
  }



}
