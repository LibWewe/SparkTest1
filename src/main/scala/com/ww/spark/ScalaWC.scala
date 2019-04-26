package com.ww.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 执行命令
  * /root/apps/spark-2.2.3/bin/spark-submit --master spark://wewe:7077,wewe2:7077 --class com.ww.spark.ScalaWC /root/code/SparkTest-1.0.jar hdfs://wewe:9000/spark/input hdfs://wewe:9000/spark/wcout01
  */
object ScalaWC {
    def main(args: Array[String]): Unit = {
        //创建spark配置
        val sparkConf = new SparkConf()
        sparkConf.setAppName("ScalaWC").setMaster("local[4]")
        //setMaster("local[4]"),添加该设置可以使该spark程序在本地运行，可用于测试
        sparkConf.setAppName("ScalaWC").setMaster("local[4]")
        //创建spark执行的入口
        val context: SparkContext = new SparkContext(sparkConf)
        //从文件中读取数据
        val lines: RDD[String] = context.textFile(args(0))
        //将一行数据按单词进行切分
        val words: RDD[String] = lines.flatMap(_.split(" "))
        //将单词和1进行组合
        val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
        //按单词进行聚合
        val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        //按照数量从多到少进行排序
        val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
        //讲结果进行保存
        sorted.saveAsTextFile(args(1))
        //释放资源
        context.stop()

    }
}
