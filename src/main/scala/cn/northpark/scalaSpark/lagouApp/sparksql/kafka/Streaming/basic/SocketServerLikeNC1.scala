package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}

import scala.util.Random

// 每秒发送1个数字，从1开始
object SocketServerLikeNC1 {
  def main(args: Array[String]): Unit = {
    val port: Int = 9999

    val server = new ServerSocket(port)
    val socket: Socket = server.accept()
    println("成功连接到本地主机：" + socket.getInetAddress)
    var i = 0
    while (true) {
      i += 1
      val out = new PrintWriter(socket.getOutputStream)
      out.println(i)
      out.flush()
      Thread.sleep(1000)
    }
  }
}