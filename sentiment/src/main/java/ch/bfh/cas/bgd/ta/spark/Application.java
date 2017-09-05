package ch.bfh.cas.bgd.ta.spark;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import ch.bfh.cas.bgd.ta.util.Review;
import scala.Serializable;
import scala.Tuple2;

public class Application implements Serializable {
	private static final long serialVersionUID = 3964335446668180836L;

	private static SparkConf sparkConf;
	private transient JavaSparkContext sparkContext;
	
	private static Logger logger = LogManager.getLogger(Application.class);

	static {
		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 0) {
			System.out.println("Usage: Application");
			System.exit(0);
		}

		Application app = new Application();

		// 1. read data 
		logger.info("reading data...");
		JavaRDD<Review> reviews = app.readData();
		logger.info("read " + reviews.count() + " reviews");
		
		// your code here
		
	}

	public Application() {
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaRDD<Review> readData() {
		JavaRDD<Review> reviews = readData(Configuration.DATA_DIR + "/neg", Configuration.SCORE_NEGATIVE);
		return reviews.union(readData(Configuration.DATA_DIR + "/pos", Configuration.SCORE_POSITIVE));
	}
	
	private JavaRDD<Review> readData(String path, double gsValue) {
		JavaPairRDD<String, String> files = sparkContext.wholeTextFiles(path);
		JavaPairRDD<String, Review> reviewsPair = files
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Review>() {
					private static final long serialVersionUID = 6183921807258769850L;

					public Tuple2<String, Review> call(Tuple2<String, String> review) throws Exception {
						return new Tuple2<String, Review>(review._1(),
								new Review(review._1(), review._2(), gsValue));
					}
				});
		JavaRDD<Review> reviews = reviewsPair.values();
		return reviews;
	}
}
