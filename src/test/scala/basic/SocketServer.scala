package basic

import java.net.ServerSocket

object SocketServer {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println(s"Socket Server 已启动：${server.getInetAddress}:${server.getLocalPort}")

    while (true) {
        val socket = server.accept()
        println("成功连接到本地主机：" + socket.getInetAddress)
        new ServerThread(socket).start()
    }
  }
}
