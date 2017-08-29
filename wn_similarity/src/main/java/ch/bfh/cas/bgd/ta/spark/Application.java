package ch.bfh.cas.bgd.ta.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.didion.jwnl.JWNLException;
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

	public static void main(String[] args) throws IOException, JWNLException {
		// your code here
		
	}
}
