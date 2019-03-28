package com.eki;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class InfoAvg {
    public static void main(String[] args) {
        //local[3]表示使用3个线程进行计算
        SparkConf sparkConf = new SparkConf().setAppName("AvgCalculator").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //读取文件
        JavaRDD<String> datafile = sc.textFile("/home/xiaoxing/IdeaProjects/HightStat/info.csv");

        //获取【性别,身高】键值对
        JavaPairRDD<String, Integer> totalHeights = datafile.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s.split(",")[1], Integer.parseInt(s.split(",")[2]));
                    }
                }
        );

        JavaRDD<Tuple2<String, Integer>> rdd = datafile.map(
                new Function<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s.split(",")[1], Integer.parseInt(s.split(",")[2]));
                    }
                }
        );

        //将totalHeights的RDD保存入内存中,以便多次使用
        totalHeights.persist(StorageLevel.MEMORY_ONLY());

        //查看totalHeights的RDD分区数
        System.out.println(totalHeights.partitions().size());

        //获取男性与女性的总身高
        JavaPairRDD<String, Integer> resultRDD = totalHeights.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );

        List<Tuple2<String, Integer>> list = totalHeights.take(10);
        for (Tuple2<String, Integer> item : list) {
            System.out.println(item.toString());
        }

        /**
         * 以下为使用aggregate计算平均身高
         * 来自于“Spark快速大数据编程”35页
         */
        //把JavaPairRDD中的元素合并起来放入累加器
        Function2<AvgCount, Tuple2<String, Integer>, AvgCount> addAndCount =
                new Function2<AvgCount, Tuple2<String, Integer>, AvgCount>() {
                    public AvgCount call(AvgCount avgCount, Tuple2<String, Integer> integer) throws Exception {
                        avgCount.total += integer._2;
                        avgCount.num += 1;
                        return avgCount;
                    }
                };

        //考虑到分布式部署，combine函数将累加器两两合并
        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                        a.total += b.total;
                        a.num += b.num;
                        return a;
                    }
                };

        final AvgCount initial = new AvgCount(0,0);
        AvgCount result = totalHeights.aggregate(initial, addAndCount, combine);
        System.out.println(result.num + "人平均身高： " + result.avg());

        /**
         * 以下为使用reduceByKey()与mapValues()计算男性/女性平均身高
         * 来自于“Spark快速大数据编程”45页
         */
        //形成[性别 (身高,1)]格式的数据, 身高为key, (身高, 1)为value
        JavaPairRDD<String, Tuple2<Integer, Integer>> mapValues = totalHeights.mapValues(
                new Function<Integer, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                        return new Tuple2<Integer, Integer>(integer, 1);
                    }
                }
        );

        //形成[性别 (总身高,人数)]格式的数据, 身高为key, (总身高,人数)为value
        JavaPairRDD<String, Tuple2<Integer, Integer>> reduceResult = mapValues.reduceByKey(
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        return new Tuple2<Integer, Integer>(t1._1 + t2._1, t1._2 + t2._2);
                    }
                }
        );

        //进行计算，形成[性别 平均身高]的PairRDD
        JavaPairRDD<String, Double> result2 = reduceResult.mapValues(
                new Function<Tuple2<Integer, Integer>, Double>() {
                    public Double call(Tuple2<Integer, Integer> tuple) throws Exception {
                        return tuple._1 / (double) tuple._2;
                    }
                }
        );

        System.out.println(result2.collect());

        /**
         * 以下为使用conbineByKeys()计算男性/女性平均身高
         * 来自于“Spark快速大数据编程”47页
         */
        //遇到新的key时创建累加器的初始值
        Function<Integer, AvgCount> createAcc =
                new Function<Integer, AvgCount>() {
                    public AvgCount call(Integer integer) throws Exception {
                        return new AvgCount(integer, 1);
                    }
                };

        //遇到已经存在的键，将该键对应的累加器与新的值进行合并
        Function2<AvgCount, Integer, AvgCount> addAndCount2 =
                new Function2<AvgCount, Integer, AvgCount>() {
                    public AvgCount call(AvgCount avgCount, Integer integer) throws Exception {
                        avgCount.total += integer;
                        avgCount.num += 1;
                        return avgCount;
                    }
                };

        //考虑到分布式部署，combine函数将累加器两两合并
        Function2<AvgCount, AvgCount, AvgCount> combine2 =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                        a.total += b.total;
                        a.num += b.num;
                        return a;
                    }
                };

        JavaPairRDD<String, AvgCount> avgCounts = totalHeights.combineByKey(createAcc, addAndCount2, combine2);
        Map<String, AvgCount> result3 = avgCounts.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : result3.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().avg());
        }

        /**
         * 累加器练习
         * 来自于“Spark快速大数据编程”89页
         */

        //身高大于200的累加器
        final Accumulator<Integer> giant = sc.accumulator(0);

        //获取【性别,身高】键值对
//        JavaPairRDD<String, Integer> giants = datafile.mapToPair(
//                new PairFunction<String, String, Integer>() {
//                    public Tuple2<String, Integer> call(String s) throws Exception {
//                        //如果身高大于200则在giant累加器中加1
//                        if (Integer.parseInt(s.split(",")[2]) >= 200) {
//                            giant.add(1);
//                            return new Tuple2<String, Integer>(s.split(",")[1], Integer.parseInt(s.split(",")[2]));
//                        } else {
//                            return new Tuple2<String, Integer>(s.split(",")[1], 0);
//                        }
//                    }
//                }
//        );

        datafile.foreach(
                new VoidFunction<String>() {
                    public void call(String s) throws Exception {
                        if (Integer.parseInt(s.split(",")[2]) >= 200) {
                            giant.add(1);
                        }
                    }
                }
        );

        //Spark为惰性计算，必须进行action后才会进行累加
        //如果是giants.take(5)，并不会进行全文的计算
//        giants.saveAsTextFile("/home/xiaoxing/IdeaProjects/HightStat/result.txt");


        System.out.println("共有" + giant.value() + "人身高超过2米");


        /**
         * 使用StatsCounter计算男性身高平均及标准差
         * 来自于“Spark快速大数据编程”99页
         */
        //StatsCounter只能在JavaDoubleRDD下使用，必须使用mapToDouble转化
        JavaDoubleRDD maleHeightDoubles = totalHeights.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple._1.equals("M");
                    }
                }
        ).mapToDouble(
                new DoubleFunction<Tuple2<String, Integer>>() {
                    public double call(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple._2;
                    }
                }
        );

        final StatCounter maleStats = maleHeightDoubles.stats();
        final Double stddev = maleStats.stdev();
        final Double mean = maleStats.mean();

        System.out.println("average is " + mean + " and stdev is " + stddev);

        /**
         * 打印maleHeightDoubles的RDD的谱系
         * 来自于“Spark快速大数据编程”128页
         */
        System.out.println(maleHeightDoubles.toDebugString());


        //将内存中保存的totalHeights的RDD删除
        totalHeights.unpersist();
    }

}
