package cn.northpark.core.examples

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义累加器:
  * 自定义累加器需要继承extends AccumulatorV2[String, String]，第一个为输入类型，第二个为输出类型
  * val myVectorAcc = new VectorAccumulatorV2
  *  方法 zero() 要不 reset() 值保持一致
  */
case class Info(var totalCount:Int,var totalAge :Int)


class SelfAccumlator extends AccumulatorV2 [Info,Info]{

  /**
    * 初始化累计器的值,这个值是最后要在merge合并的时候累加到最终结果内
    */
  private var result: Info = new Info(0,0)
//  println(s" in first result = $result  end。")

  /**
    * 返回累计器是否是零值。 例如： Int 类型累加器 0 就是零值，对于List 类型数据 Nil 就是零值。
    * 这里判断时，要与方法reset()初始的值一致，初始判断时要返回true. 内部会在每个分区内自动调用判断。
    */
  override def isZero: Boolean = {
//    println("判断 累加器是否是初始值***"+(result.totalAge == 0 && result.totalCount ==0)+" ***end")
    result.totalCount ==0 && result.totalAge == 0
  }

  /**
    * 复制一个新的累加器,在这里就是如果用到了就会复制一个新的累加器。
    */
  override def copy(): AccumulatorV2[Info, Info] = {
    val newAccumulator = new SelfAccumlator()
    newAccumulator.result = this.result
    newAccumulator
  }

  /**
    * 重置AccumulatorV2中的数据，这里初始化的数据是在RDD每个分区内部，每个分区内的初始值。
    */
  override def reset(): Unit = {
//    println("重置累加器中的值")
    result = new Info(0,0)
  }

  /**
    * 每个分区累加数据
    * 这里是拿着初始的result值和每个分区的数据累加
    */
  override def add(v: Info): Unit = {
//    println(s" in add method : v = $v ,v.totalCount = ${v.totalCount},v.totalAge = ${v.totalAge}")
    result.totalCount += v.totalCount
    result.totalAge += v.totalAge
  }

  /**
    * 分区之间总和累加数据
    *
    * 这里拿着初始的result值 和每个分区最终的结果累加
    *
    */
  override def merge(other: AccumulatorV2[Info, Info]): Unit = other match {
    case o : SelfAccumlator => {
//      println(s" in merge method : o = $o ")
      result.totalCount +=o.result.totalCount
      result.totalAge +=o.result.totalAge
    }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    *  累计器堆外返回的最终的结果
    */
  override def value: Info = result
}


object DefindSelfAccumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("selfAccumulator")
    val sc = new SparkContext(conf)

    val nameList = sc.parallelize(List[String](
      "A 1","B 2","C 3",
      "D 4","E 5","F 6",
      "G 7","H 8","I 9"
    ),3)
    println("nameList RDD partition length = "+nameList.getNumPartitions)
    /**
      * 初始化累加器
      *
      */
    val myAccumulator = new SelfAccumlator()
    sc.register(myAccumulator, "First Accumulator")

    val transInfo = nameList.map(one=>{
      val info = Info(1,one.split(" ")(1).toInt)
      myAccumulator.add(info)
      info
    })

    transInfo.count()

    println(s"accumulator totalCount = ${myAccumulator.value.totalCount}, totalAge = ${myAccumulator.value.totalAge}")





  }
}
