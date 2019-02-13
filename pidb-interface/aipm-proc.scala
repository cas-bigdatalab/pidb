import java.io.InputStream

import scala.collection.mutable.ArrayBuffer

//this is a scala script
//will be called by AipmTest as an external process

//read 8 bytes
def readLong(is: InputStream): Long = {
  val bytes = readBytes(is, 8);

  val longValue = 0L |
    ((bytes(0) & 0xff).toLong << 56) |
    ((bytes(1) & 0xff).toLong << 48) |
    ((bytes(2) & 0xff).toLong << 40) |
    ((bytes(3) & 0xff).toLong << 32) |
    ((bytes(4) & 0xff).toLong << 24) |
    ((bytes(5) & 0xff).toLong << 16) |
    ((bytes(6) & 0xff).toLong << 8) |
    ((bytes(7) & 0xff).toLong << 0);

  longValue;
}

def readBytes(is: InputStream, n: Int): Array[Byte] = {
  val bytes: Array[Byte] = new Array[Byte](n).map(x => 0.toByte);
  val nread = is.read(bytes);

  bytes;
}

var n = 0L;
val lens = ArrayBuffer[Long]();
do {
  n = readLong(System.in);
  if (n >= 0) {
    val bs = readBytes(System.in, n.toInt);
    lens += n;
  }
} while (n > 0)

val slens = lens.map("" + _).reduce { (a, b) =>
  "" + a + "," + b
};

println(s"""{"lengths": [$slens]}""");

