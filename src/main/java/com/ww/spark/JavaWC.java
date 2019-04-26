package com.ww.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWC {
    public static void main(String[] args) {
        //创建设置
        SparkConf conf = new SparkConf();
        //设置程序名称
        conf.setAppName("JavaWC");
        //设置本地调试执行
        conf.setMaster("local[4]");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //从指定的路径获取文件
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //将一行数据进行切分
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //将单词和1组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        //将相同的单词进行聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //将单词和聚合后的数字对换位置
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        //进行排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //再将聚合数字与单词对换位置
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        //讲结果进行保存
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();


    }
}
