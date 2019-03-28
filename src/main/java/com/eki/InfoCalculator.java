package com.eki;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 对身高csv文件进行分析
 */
public class InfoCalculator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("InfoCalulator").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //读取文件
        //D:\gitRepo\SparkDemo\info.csv
        JavaRDD<String> datafile = sc.textFile("/home/xiaoxing/IdeaProjects/HightStat/info.csv");

        //过滤得到性别为M的条目
        JavaRDD<String> maleFilter = datafile.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String s) throws Exception {
                        return s.contains("M");
                    }
                }
        );

        //过滤得到性别为F的条目
        JavaRDD<String> femaleFilter = datafile.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String s) throws Exception {
                        return s.contains("F");
                    }
                }
        );

        //得到身高数据
        JavaRDD<String> maleHeightData = maleFilter.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")[2]).iterator();
                    }
                }
        );

        JavaRDD<String> femaleHeightData = femaleFilter.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")[2]).iterator();
                    }
                }
        );

        //将string类型的身高数据转换为Integer类型，方便排序
        JavaRDD<Integer> maleHeight = maleHeightData.map(
                new Function<String, Integer>() {
                    public Integer call(String s) throws Exception {
                        return Integer.parseInt(String.valueOf(s));
                    }
                }
        );

        JavaRDD<Integer> femaleHeight = femaleHeightData.map(
                new Function<String, Integer>() {
                    public Integer call(String s) throws Exception {
                        return Integer.parseInt(String.valueOf(s));
                    }
                }
        );

        //升序排列，first()得到最矮身高
        JavaRDD<Integer> maleSortAsc = maleHeight.sortBy(
                new Function<Integer, Integer>() {
                    public Integer call(Integer integer) throws Exception {
                        return integer;
                    }
                },true,3
        );

        JavaRDD<Integer> femaleSortAsc = femaleHeight.sortBy(
                new Function<Integer, Integer>() {
                    public Integer call(Integer integer) throws Exception {
                        return integer;
                    }
                }, true, 3
        );

        //降序排列，first()得到最高身高
        JavaRDD<Integer> maleSortDsc = maleHeight.sortBy(
                new Function<Integer, Integer>() {
                    public Integer call(Integer integer) throws Exception {
                        return integer;
                    }
                },false,3
        );

        JavaRDD<Integer> femaleSortDsc = femaleHeight.sortBy(
                new Function<Integer, Integer>() {
                    public Integer call(Integer integer) throws Exception {
                        return integer;
                    }
                }, false, 3
        );

        System.out.println("Total male: " + maleFilter.count());
        System.out.println("Total female: " + femaleFilter.count());
        System.out.println("male lowest: " + maleSortAsc.first());
        System.out.println("female lowest: " + femaleSortAsc.first());
        System.out.println("male highest: " + maleSortDsc.first());
        System.out.println("female highest: " + femaleSortDsc.first());

    }
}
