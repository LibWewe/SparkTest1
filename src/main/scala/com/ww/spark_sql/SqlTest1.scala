package com.ww.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author wewe 
 * @ClassName SqlTest1
 * @Description //TODO spark sql v2.x版本的测试程序
 * @Date 15:32 2019/5/9
 **/
object SqlTest1 {
    def main(args: Array[String]): Unit = {
        //spark2.x SQL编程API（SparkSession）
        //SparkSession是spark2.x的SQL执行入口
        val session: SparkSession = SparkSession.builder()
                .appName("SqlTest1")
                .master("local[*]")
                .getOrCreate()

        //创建RDD
        val lines: RDD[String] = session.sparkContext.textFile("hdfs://wewe:9000/spark/person")
        //整理数据
        val rowRDD: RDD[Row] = lines.map(line => {
            val fields = line.split(",")
            val id: Long = fields(0).toLong
            val name: String = fields(1)
            val age: Int = fields(2).toInt
            val fv: Double = fields(3).toDouble
            Row(id, name, age, fv)
        })

        //结果类型，其实就是表头，用于描述DataFrame
        val schema: StructType = StructType(List(
            StructField("id", LongType, true),
            StructField("name", StringType, true),
            StructField("age", IntegerType, true),
            StructField("fv", DoubleType, true)
        ))

        //创建DataFrame
        val df: DataFrame = session.createDataFrame(rowRDD, schema)

        import session.implicits._
        val df2: Dataset[Row] = df.orderBy($"fv" desc, $"age" asc).where($"fv" > 98)

        df2.show()

        session.stop()

    }
}
