package cn.itcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 编写单词计数
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象，设置appName和Master地址
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    // 2.创建SparkContext对象，它是所有任务计算的源头
    // 它会创建DAGScheduler和TaskScheduler
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 3.读取数据文件，RDD可以简单地理解为是一个集合
    // 集合中存放的元素是String类型
    val data: RDD[String] = sparkContext.textFile("D:\\word\\words.txt")
    // 4.切分每一行，获取所有的单词
    val words: RDD[String] = data.flatMap(_.split(" "))
    // 5.每个单词记为1，转换为(单词, 1)
    val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
    // 6.相同单词汇总，前一个下划线表示累加数据，后一个下划线表示新数据
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    // 7.收集打印结果数据
    val finalResult: Array[(String, Int)] = result.collect()
    println(finalResult.toBuffer)
    // 8.关闭sparkContext对象
    sparkContext.stop()
  }

}
