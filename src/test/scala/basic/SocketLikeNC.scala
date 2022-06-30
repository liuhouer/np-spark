package basic

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}

import scala.util.Random

object SocketLikeNC {
  def main(args: Array[String]): Unit = {
    val words: Array[String] = "Hello World Hello Hadoop Hello spark kafka hive zookeeper hbaseflume sqoop".split("\\s+")
    val n: Int = words.length
    val port: Int = 9999
    val random: Random = scala.util.Random

    val server = new ServerSocket(port)
    val socket: Socket = server.accept()
    println("成功连接到本地主机：" + socket.getInetAddress)
    while (true) {
      val out = new PrintWriter(socket.getOutputStream)
      out.println(words(random.nextInt(n)) + " "+ words(random.nextInt(n)))
      out.flush()
      Thread.sleep(100)
    }
  }
}
