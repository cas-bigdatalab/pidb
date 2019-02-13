package cn.aipm.service

import java.io.{File, FileInputStream}

import org.junit.{Assert, Test}


class ServicesTest {
  @Test
  def test1(): Unit = {
    var image_path1 = "E:/[face]/unknown/test.jpg"
    var image_path2 = "E:/[face]/unknown/test.jpg"

    val sim = Services.computeFaceSimilarity(image_path1, image_path2)
    print(sim)
  }

  @Test
  def test2(): Unit = {
    var image_path1 = "E:/[face]/unknown/test.jpg"
    var image_path2 = "E:/[face]/unknown/test2.jpg"
//    Services.hostUrl = "http://127.0.0.1:8081/service/"
    val sim = Services.computeFaceSimilarity(image_path1, image_path2)
    print(sim)
  }

  @Test
  def test3(): Unit = {
    var image_path1 = "E:/[face]/unknown/test.jpg"
    var image_path2 = "E:/[face]/unknown/test2.jpg"
    val file1 = new File(image_path1)
    val file2 = new File(image_path2)
    val in1 = new FileInputStream(file1)
    val in2 = new FileInputStream(file2)
//    Services.hostUrl="http://127.0.0.1:8081/service/"
    val sim = Services.computeFaceSimilarity(in1, in2)
    print(sim)
  }
}
