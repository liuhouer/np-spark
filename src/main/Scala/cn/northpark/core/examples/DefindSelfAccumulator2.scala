package cn.northpark.core.examples

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

class MyAcc extends AccumulatorV2[String,String]{

//  var returnResult = "ppp"
  var returnResult = ""


  override def isZero: Boolean = {
    "X".equals(returnResult)
  }

  override def copy(): AccumulatorV2[String, String] = {
    val myAcc = new MyAcc
    myAcc.returnResult = this.returnResult
    myAcc
  }

  override def reset(): Unit = {
    returnResult = "X"
  }

  override def add(v: String): Unit = {
    returnResult += v
  }


  override def merge(other: AccumulatorV2[String, String]): Unit = {
    returnResult += other.asInstanceOf[MyAcc].returnResult
  }

  override def value: String = returnResult
}

object DefindSelfAccumulator2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("selfAccumulator2")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(List[String]("a","b","c","d","e","f"),5)

    /**
      * 定义累计器
      */
    val myacc = new MyAcc()
    sc.register(myacc,"myacc")

    infos.map(one=>{
      myacc.add(one)
    }).count()
    println(s" 累计器值 = ${myacc.value}")
  }
}
