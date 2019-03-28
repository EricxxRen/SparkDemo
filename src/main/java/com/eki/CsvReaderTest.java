package com.eki;

import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Iterator;

/**
 * 暂时有问题，不要看
 */
public class CsvReaderTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("InfoCalulator").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, String> csvData = sc.wholeTextFiles("/home/xiaoxing/IdeaProjects/HightStat/info.csv");

        class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
            public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
                CSVReader csvReader = new CSVReader(new StringReader(file._2()));
                return csvReader.readAll().iterator();
            }
        }

//        JavaRDD<String[]> keyedRDD = csvData.flatMap(
//                new FlatMapFunction<Tuple2<String, String>, String[]>() {
//                    public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
//                        CSVReader csvReader = new CSVReader(new StringReader(file._2()));
//                        return csvReader.readAll().iterator();
//                    }
//                }
//        );

        JavaRDD<String[]> keyedRDD2 = csvData.flatMap(new ParseLine());

        System.out.println(keyedRDD2.take(5));
    }
}
