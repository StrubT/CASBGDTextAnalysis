package ch.bfh.cas.bgd.ta;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
		
		// now it is your turn :)
	}
}
