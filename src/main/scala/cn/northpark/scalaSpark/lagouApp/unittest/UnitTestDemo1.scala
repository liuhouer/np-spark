package cn.northpark.scalaSpark.lagouApp.unittest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class UnitTestDemo1 extends FlatSpec with BeforeAndAfter {
  val master = "local" //sparkcontext的运行master
  var sc: SparkContext = _
  it should("test success") in{
    //其中参数为rdd或者dataframe可以通过通过简单的手动构造即可
    val seq = Seq("the test test1","the test","the")
    val rdd: RDD[String] = sc.parallelize(seq)
    val wordCounts: RDD[(String, Int)] = WordCount.count(rdd)

    wordCounts.map(p => {
      p._1 match {
        case "the"=>
          assert(p._2==3)
        case "test"=>
          assert(p._2==2)
        case "test1"=>
          assert(p._2==1)
        case _=>
          None
      }
    }).foreach(_=>())
  }
  //这里before和after中分别进行sparkcontext的初始化和结束
  before {
    val conf = new SparkConf()
      .setAppName("test").setMaster(master)
    sc=new SparkContext(conf)
  }

  after{
    if(sc!=null){
      sc.stop()
    }
  }
}

// scalatest_2.12 版本很重要，3.0.x有问题，任务不能序列化。是由 assert 引起的，换成 3.1.x 就好了
// 这个问题也有点奇怪，为什么 assert 需要序列化？！