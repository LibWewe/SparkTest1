package com.ww.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
/*
 * @Author wewe
 * @ClassName SqlDemo1
 * @Description //TODO spark sql v1.x版本的测试，使用sql方式并创建case class
 * @Date 15:27 2019/5/9
 **/
object SqlDemo1 {
    def main(args: Array[String]): Unit = {
        //创建spark config
        val conf: SparkConf = new SparkConf()
        conf.setAppName("SqlDemo1")
        conf.setMaster("local[2]")
        //创建spark sql 连接
        val sc: SparkContext = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        //获取hdfs中的数据
        val lines: RDD[String] = sc.textFile("hdfs://wewe:9000/spark/person")
        val babyRDD: RDD[Baby] = lines.map(line => {
            val fields = line.split(",")
            val id: Long = fields(0).toLong
            val name: String = fields(1)
            val age: Int = fields(2).toInt
            val fv: Double = fields(3).toDouble
            Baby(id, name, age, fv)
        })
        //将RDD转换成DataFrame
        import sqlContext.implicits._
        val bdf: DataFrame = babyRDD.toDF
        //把DataFrame注册成临时表
        bdf.registerTempTable("t_baby")
        //写sql语句
        val result: DataFrame = sqlContext.sql("SELECT * FROM t_baby")
        //查看结果
        result.show()
        //回收资源
        sc.stop()

    }
}

case class Baby(id: Long, name: String, age: Int, fv: Double)