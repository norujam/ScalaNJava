package future

import java.io._
import java.util
import java.util.zip.{ZipEntry, ZipInputStream}

import scala.util.control.Breaks

class ScalaZipFileHandler(file:File) {

  def call() : util.Map[String, String] = {
    System.out.println(Thread.currentThread.getName)
    var zipIn: ZipInputStream = null
    var entry: ZipEntry = null
    val result:util.Map[String, String] = new util.HashMap[String, String]
    try {
      zipIn = new ZipInputStream(new FileInputStream(file))
      entry = zipIn.getNextEntry
      // iterates over entries in the zip file
      while ( {
        entry != null
      }) {
        val filePath = "c:/temp/unzipFile" + File.separator + entry.getName
        if (!entry.isDirectory) { // if the entry is a file, extracts it
          extractFile(zipIn, filePath)
        }
        else { // if the entry is a directory, make the directory
          val dir = new File(filePath)
          dir.mkdir
        }
        zipIn.closeEntry()
        entry = zipIn.getNextEntry
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        result.put("status", "Error-1")
        result.put("fileName", file.getAbsolutePath)
        result

    } finally if (zipIn != null) try {
      zipIn.closeEntry()
      zipIn.close()
    } catch {
      case ie: IOException =>
        ie.printStackTrace()
    }

    result.put("status", "1")
    result.put("fileName", file.getAbsolutePath)

    result
  }

  @throws[IOException]
  private def extractFile(zipIn: ZipInputStream, filePath: String): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(filePath))
    val bytesIn = new Array[Byte](1024)
    var read = 0
    val loop = new Breaks;
    loop.breakable {
      while ((read = zipIn.read(bytesIn)) != -1) {
        if (read == -1) {
          loop.break()
        }
        bos.write(bytesIn, 0, read)
      }
    }
    bos.close()
  }

}
