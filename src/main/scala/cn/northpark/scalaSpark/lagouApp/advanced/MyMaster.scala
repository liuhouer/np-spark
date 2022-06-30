//package cn.northpark.scalaSpark.lagouApp.advanced
//
//import org.apache.spark.SparkConf
//import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
//import org.apache.spark.SecurityManager
//
//class MyMaster(override val rpcEnv: RpcEnv,
//              address: RpcAddress,
//              val securityMgr: SecurityManager,
//              val conf: SparkConf)
//  extends ThreadSafeRpcEndpoint {
//  println("MyMaster 主构造器运行")
//
//  override def receive: PartialFunction[Any, Unit] = {
//    case TestAdd(x, y) => println(s"x=$x, y=$y; x+y=${x+y}")
//
//    // 接收注册信息，恢复以及注册
//    case RegisterWorker(id, workerHost, workerPort, workerRef, cores, memory, masterAddress) =>
//      println(s"id = $id, workerHost = $workerHost, workerPort = $workerPort, workerRef = $workerRef, cores = $cores, memory = $memory, masterAddress  = $masterAddress ")
//      workerRef.send(RegisteredWorker(self, masterAddress))
//
//    case Heartbeat(workerId, worker) =>
//      println(s"workerId = $workerId, worker = $worker, time = ${System.currentTimeMillis()}")
//
//    case _ => println("MyMaster 接收到未知消息")
//  }
//
//  override def onStart(): Unit = {
//    println("My Master is onStart starting ... ...")
//  }
//}
//
//object MyMaster{
//  val SYSTEM_NAME = "MyMaster"
//  val ENDPOINT_NAME = "Master"
//
//  def main(argStrings: Array[String]): Unit = {
//    val host = "localhost"
//    val port = 9999
//    val conf = new SparkConf
//
//    val securityMgr = new SecurityManager(conf)
//    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
//
//    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
//      new MyMaster(rpcEnv, rpcEnv.address, securityMgr, conf))
//    masterEndpoint.send(TestAdd(10, 20))
//
//    rpcEnv.awaitTermination()
//  }
//}
//
//case class TestAdd(x: Int, y: Int)
//case class RegisterWorker(id: String,
//                          host: String,
//                          port: Int,
//                          worker: RpcEndpointRef,
//                          cores: Int,
//                          memory: Int,
//                          masterAddress: RpcAddress)
//case class RegisteredWorker(master: RpcEndpointRef,
//                            masterAddress: RpcAddress)
//case object SendHeartbeat
//
//case class Heartbeat(workerId: String, worker: RpcEndpointRef)