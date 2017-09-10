package ch.bfh.cas.bgd.ta.spark;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;

public class Application implements Serializable {
	private static final long serialVersionUID = -3539379795242508287L;

	private static SparkConf sparkConf;
	
	private static JavaSparkContext sparkContext;
	
	private static Logger logger = LogManager.getLogger(Application.class);

	static {
		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public static JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public Application() {
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 0) {
			System.out.println("Usage: Application");
			System.exit(0);
		}

		Application app = new Application();
		
		// your code here
	}
}
