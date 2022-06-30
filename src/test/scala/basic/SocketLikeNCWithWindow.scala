package basic

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}

object SocketLikeNCWithWindow {
  def main(args: Array[String]): Unit = {
    val port = 1521
    val ss = new ServerSocket(port)
    val socket: Socket = ss.accept()
    println("connect to host : " + socket.getInetAddress)
    var i = 0
    // 每秒发送1个数

    while(true) {
      i += 1
      val out = new PrintWriter(socket.getOutputStream)
      out.println(i)
      out.flush()
      Thread.sleep(1000)
    }
  }
}