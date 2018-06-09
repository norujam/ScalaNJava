package future

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util._
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

object ScalaFileMonitor {
  private var fileList : Array[File] = null

  def main(args: Array[String]): Unit = {
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

    val targetList: util.List[java.io.File] = Collections.synchronizedList(new util.ArrayList[File])
    val runningList: util.List[java.io.File] = Collections.synchronizedList(new util.ArrayList[File])

    try {

      val task = new TimerTask() {
        override def run(): Unit = {
          val startTime = System.currentTimeMillis
          System.out.println("Monitor is running...");
          val targetDir = new File("c:/temp")
          if (targetDir.isDirectory) {
            fileList = targetDir.listFiles
            if (fileList == null) return
            for (file <- fileList) {
              if (file.isFile && targetList.size < 4 && !targetList.contains(file) && file.getName.toLowerCase.contains(".zip"))
                targetList.add(file)
            }
          }
          if (targetList.isEmpty) return

          val futures = ArrayBuffer.empty[Future[util.Map[String, String]]]

          for (i <- 0 to targetList.size()-1) {
            if(!(runningList.contains(targetList.get(i)))) {
              futures += Future {
                val zipFileHandler = new ScalaZipFileHandler(targetList.get(i))
                zipFileHandler.call()
              }
              runningList.add(targetList.get(i))
            }
          }

          for (f <- futures) {
            println("future " + Await.result(f, Duration.Inf).get("fileName"))
            if(Await.result(f, Duration.Inf).get("status").equals("1")) {
              val doneFile = new File(Await.result(f, Duration.Inf).get("fileName"))
              runningList.remove(doneFile)
              targetList.remove(doneFile)
              doneFile.delete
            } else {
              val errorFile = new File(Await.result(f, Duration.Inf).get("fileName"))
              copyFile(errorFile, new File("c:/temp/errFile/"))
              runningList.remove(errorFile)
              targetList.remove(errorFile)
              errorFile.delete
            }

          }

          val stopTime = System.currentTimeMillis
          val elapsedTime = stopTime - startTime
          println("run time="+elapsedTime)
        }
      }

      val timer = new Timer
      val delay = 0
      val intevalPeriod = 1 * 10000
      // schedules the task to be run in an interval
      timer.scheduleAtFixedRate(task, delay, intevalPeriod)

    } catch {
            case e:Exception => {e.printStackTrace()}
    }
  }

  @throws[Exception]
  private def copyFile(source: File, target: File): Unit = {
    if (source.isDirectory) {
      if (!target.isDirectory) target.mkdirs
      val children = source.list
      var i = 0
      while ( {
        i < children.length
      }) {
        copyFile(new File(source, children(i)), new File(target, children(i)))

        {
          i += 1; i - 1
        }
      }
    }
    else {
      val bufferSize = 8192
      //(64 * 1024 * 1024) - (32 * 1024);
      val inChannel = new FileInputStream(source).getChannel
      if (target.isDirectory) {
        System.out.println(target.getAbsolutePath)
        new File(target.getAbsolutePath + File.separator + source.getName)
      }
      val outChannel = new FileOutputStream(target).getChannel
      try {
        val byteBuffer = ByteBuffer.allocateDirect(bufferSize)
        while ( {
          inChannel.read(byteBuffer) != -1
        }) {
          byteBuffer.flip
          outChannel.write(byteBuffer)
          byteBuffer.clear
        }
      } catch {
        case e: Exception =>
          throw e
      } finally {
        if (inChannel != null) inChannel.close()
        if (outChannel != null) outChannel.close()
      }
    }
  }
}

