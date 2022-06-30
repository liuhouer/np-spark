//package cn.northpark.scalaSpark.lagouApp.advanced
//
//import java.text.SimpleDateFormat
//import java.util.concurrent.TimeUnit
//import java.util.{Date, Locale}
//
//import org.apache.spark.SparkConf
//import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
//import org.apache.spark.SecurityManager
//import org.apache.spark.util.{ThreadUtils, Utils}
//
//class MyWorker( override val rpcEnv: RpcEnv,
//                cores: Int,
//                memory: Int,
//                masterRpcAddresses: Array[RpcAddress],
//                endpointName: String,
//                val conf: SparkConf,
//                val securityMgr: SecurityManager)
//  extends ThreadSafeRpcEndpoint {
//  println("MyWorker 主构造器运行 ... ...")
//
//  // 定义参数
//  private val host = rpcEnv.address.host
//  private val port = rpcEnv.address.port
//  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
//  private var master: Option[RpcEndpointRef] = None
//
//  private val workerId = generateWorkerId()
//  private def generateWorkerId(): String = {
//    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
//  }
//
//  private val forwordMessageScheduler =
//    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
//  // 发送心跳间隔时间。15s发送一次心跳
//  private val HEARTBEAT_MILLIS = 60 * 1000 / 4
//
//  override def onStart(): Unit = {
//    println("My Worker is onStart 向 MyMaster 注册 ... ...")
//    masterRpcAddresses.foreach { masterAddress =>
//      val masterEndpoint: RpcEndpointRef = rpcEnv.setupEndpointRef(masterAddress, MyMaster.ENDPOINT_NAME)
//      masterEndpoint.send(TestAdd(100, 200))
//      masterEndpoint.send(RegisterWorker(
//        workerId,
//        host,
//        port,
//        self,
//        cores,
//        memory,
//        masterEndpoint.address))
//    }
//  }
//
//  override def receive: PartialFunction[Any, Unit] = {
//    case TestAdd(x, y) => println(s"x=$x, y=$y; x+y=${x+y}")
//
//    // 收到注册信息的回复。发送心跳
//    case RegisteredWorker(master, masterAddress) =>
//      println(s"master = $master, masterAddress = $masterAddress")
//      this.master = Some(master)
//      forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
//        override def run(): Unit = Utils.tryLogNonFatalError {
//          self.send(SendHeartbeat)
//        }
//      }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
//
//    // 自己给自己发送的消息
//    case SendHeartbeat =>
//      master.get.send(Heartbeat(workerId, self))
//
//    case _ => println("MyWorker 接收到未知消息")
//  }
//
//}
//
//object MyWorker{
//  val SYSTEM_NAME = "MyWorker"
//  val ENDPOINT_NAME = "Worker"
//
//  def main(argStrings: Array[String]) {
//    val host = "localhost"
//    val port = 8888
//    val cores = 8
//    val memory = 1000000
//    val masters: Array[String] = Array("spark://localhost:9999")
//
//    val conf = new SparkConf
//
//    val systemName: String = SYSTEM_NAME
//    val securityMgr = new SecurityManager(conf)
//    val rpcEnv: RpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
//    val masterAddresses: Array[RpcAddress] = masters.map(RpcAddress.fromSparkURL(_))
//    rpcEnv.setupEndpoint(ENDPOINT_NAME, new MyWorker(rpcEnv, cores, memory,
//      masterAddresses, ENDPOINT_NAME, conf, securityMgr))
//
//    rpcEnv.awaitTermination()
//  }
//}