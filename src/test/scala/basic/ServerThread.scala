package basic

import java.io.DataOutputStream
import java.net.Socket

class ServerThread(sock: Socket) extends Thread {
  val words = "hello world hello spark hello word hello java hello hadoop hello kafka"
    .split("\\s+")
  val length = words.length

  override def run(): Unit = {
    val out = new DataOutputStream(sock.getOutputStream)
    val random = scala.util.Random

    while (true) {
      val (wordx, wordy) = (words(random.nextInt(length)), words(random.nextInt(length)))
      out.writeUTF(s"$wordx $wordy")
      Thread.sleep(100)
    }
  }
}