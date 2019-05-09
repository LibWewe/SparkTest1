package com.ww.spark_sql


import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
 * @Author wewe
 * @ClassName SqlWC
 * @Description //TODO spark sql v2.x的DSL的方式实现word count
 * @Date 16:28 2019/5/9
 **/
object DSLWC {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("DSLWC")
                .master("loacl[*]")
                .getOrCreate()
        //读数据，lazy,Dataset也是分布式数据集，但是更加智能，是对DataFrame的进一步封装
        val lines: Dataset[String] = sparkSession.read.textFile("hdfs://wewe:9000/spark/wc1")
        //导入sparkSession的隐式转换
        import sparkSession.implicits._
        //将读到的数据进行切分压平
        val words: Dataset[String] = lines.flatMap(_.split(" "))
        //分组
        //val result: DataFrame = words.groupBy($"value" as "word").count().sort("word")
        import org.apache.spark.sql.functions._
        val result: DataFrame = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)
        //获取结果
        result.show()
        //释放资源
        sparkSession.stop()

    }
}
