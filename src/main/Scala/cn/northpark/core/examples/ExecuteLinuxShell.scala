package cn.northpark.core.examples

import java.io.{BufferedReader, InputStreamReader}

object ExecuteLinuxShell {
  def main(args: Array[String]): Unit = {
    //准备脚本
    val cmd = "sh /root/test/test.sh " + args(0) + " " + args(1)
    System.out.println("cmd = " + cmd)
    val process = Runtime.getRuntime.exec(cmd)
    /**
      * 可执行程序的输出可能会比较多，而运行窗口的输出缓冲区有限，会造成waitFor一直阻塞。
      * 解决的办法是，利用Java提供的Process类提供的getInputStream,getErrorStream方法
      * 让Java虚拟机截获被调用程序的标准输出、错误输出，在waitfor()命令之前读掉输出缓冲区中的内容。
      */
    val bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    var flag = bufferedReader.readLine()
    while (flag != null){
      System.out.println("result ---- " + flag)
      flag = bufferedReader.readLine()
    }
    bufferedReader.close()

    /**
      * 等待脚本执行完成
      */
    process.waitFor()
  }
}
