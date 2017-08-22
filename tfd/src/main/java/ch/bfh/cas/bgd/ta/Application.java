package ch.bfh.cas.bgd.ta;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Application {

	private SparkConf sparkConf;
	private JavaSparkContext sparkContext;

	public Application() {

		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Usage: Application <file>");
			System.exit(0);
		}

		Application application = new Application();
		try (JavaSparkContext sc = application.getSparkContext()) {

			Pattern pattern = Pattern.compile("\\W+");
			List<String> lines = sc.textFile(args[0]) //
					.flatMap(s -> pattern.splitAsStream(s).iterator()) //
					.mapToPair(s -> new Tuple2<String, Integer>(s, 1)) //
					.reduceByKey((x, y) -> x + y) //
					.map(t -> t) //
					.sortBy(t -> t._2(), false, 1) //
					.map(t -> String.format("%s %d", t._1(), t._2())) //
					.collect();

			Files.write(Paths.get(Configuration.RESULT), lines, StandardCharsets.UTF_8);

		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}
}
