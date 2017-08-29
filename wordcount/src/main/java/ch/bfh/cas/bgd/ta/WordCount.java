
package ch.bfh.cas.bgd.ta;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Usage: WordCount <file>");
			System.exit(0);
		}

		// Define a configuration to use to interact with Spark
		// TODO: may need to change the master URL
		SparkConf conf = new SparkConf().setMaster("spark://bigdata-VirtualBox:7077").setAppName("Word Count App");

		// Create a Java version of the Spark Context from the configuration
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			// 1. prepare the input data
			// 1.1 load the input data, which is a text file read from the command line
			JavaRDD<String> input = sc.textFile(args[0]);

			// 1.2 read all words from the text file
			Pattern pattern = Pattern.compile("\\W+");
			JavaRDD<String> words = input.flatMap(s -> pattern.splitAsStream(s).iterator());

			// 2. map - transform the collection of words into pairs of <word, 1>
			JavaPairRDD<String, Integer> counts = words.mapToPair(s -> new Tuple2<>(s, 1));

			// 3. reduce - count the words
			JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey((x, y) -> x + y);

			// Save the word count to a text file
			reducedCounts.coalesce(1).saveAsTextFile("output");
		}
	}
}
