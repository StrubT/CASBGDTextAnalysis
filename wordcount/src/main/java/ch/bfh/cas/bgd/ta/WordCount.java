package ch.bfh.cas.bgd.ta;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Usage: WordCount <file>");
			System.exit(0);
		}

		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("spark://bigdata-VirtualBox:7077").setAppName("Word Count App"); // TODO may need to change the master URL

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 1. prepare the input data
		// 1.1 load the input data, which is a text file read from the command line
		JavaRDD<String> input = sc.textFile(args[0]);

		// 1.2 read all words from the textfile
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(" ")).iterator(); // very crude tokenization via space character
			}
		});

		// 2. map - transform the collection of words into pairs of <word, 1>
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		// 3. reduce - count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		// Save the word count to a text file
		reducedCounts.saveAsTextFile("output");
		
		// cleanup
		sc.close();
	}
}
