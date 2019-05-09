package com.ww.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo3 {
    def main(args: Array[String]): Unit = {
        //创建spark config
        val conf: SparkConf = new SparkConf()
        conf.setAppName("SqlDemo3")
        conf.setMaster("local[2]")
        //创建spark sql 连接
        val sc: SparkContext = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        //获取hdfs中的数据
        val lines: RDD[String] = sc.textFile("hdfs://wewe:9000/spark/person")
        val rowRDD: RDD[Row] = lines.map(line => {
            val fields = line.split(",")
            val id: Long = fields(0).toLong
            val name: String = fields(1)
            val age: Int = fields(2).toInt
            val fv: Double = fields(3).toDouble
            Row(id, name, age, fv)
        })

        //结果类型，其实就是表头，用于描述DataFrame
        val sch: StructType = StructType(List(
            StructField("id", LongType, true),
            StructField("name", StringType, true),
            StructField("age", IntegerType, true),
            StructField("fv", DoubleType, true)
        ))

        //将RowRDD关联schema
        val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)

        //使用方式，就不需要创建临时表了
        val result1: DataFrame = bdf.select("name", "age", "fv")

        import sqlContext.implicits._
        val result2: Dataset[Row] = result1.orderBy($"fv" desc, $"age" asc)

        //查看结果
        result2.show()
        //回收资源
        sc.stop()

    }
}

