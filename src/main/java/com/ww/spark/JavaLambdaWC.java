package com.ww.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLambdaWC {
    public static void main(String[] args) {
        //创建设置
        SparkConf conf = new SparkConf();
        conf.setAppName("JavaLambdaWC");
        conf.setMaster("local[4]");
        //创建javaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //从指定的路径获取文件
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //将一行数据进行切分
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和1组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        //将相同单词进行聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        //将单词与数字进行对换位置
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //进行排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();
        //再将数字将单词进行对换位置
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        //将最终数据保存到本地
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();


    }
}
