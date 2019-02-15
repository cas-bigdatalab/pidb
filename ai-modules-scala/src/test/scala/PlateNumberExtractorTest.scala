import java.io.File
import org.junit.{Assert, Test}

import cn.aipm.image.PlateNumberExtractor
import cn.pidb.blob.Blob


class PlateNumberExtractorTest {

  @Test
  def test1():Unit={
    var imagePath1 = "E:\\[pidb-ai-code]\\plate_number\\test4.jpg"
    val plateExtractor = new PlateNumberExtractor()
    val res = plateExtractor.extract(Blob.fromFile(new File(imagePath1)))
    print(res)
  }

  @Test
  def test2():Unit={
    var imagePath1 = "E:/[face]/unknown/test.jpg"
    val plateExtractor = new PlateNumberExtractor()
    val res = plateExtractor.extract(Blob.fromFile(new File(imagePath1)))
    print(res)
  }


}
