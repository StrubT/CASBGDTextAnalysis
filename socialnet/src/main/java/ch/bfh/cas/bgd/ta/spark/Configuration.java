package ch.bfh.cas.bgd.ta.spark;

import scala.Serializable;

public class Configuration implements Serializable {
	private static final long serialVersionUID = 6604060899716252008L;

	// Spark master node URL
	public static final String SPARK_MASTER_URL = "spark://bigdata-VirtualBox:7077";

	// application config
	public static final String APP_NAME = "socialnet";
	
	// application dir
	private static final String APP_DIR = "/home/bigdata/develop/socialnet";
	
	// graph data
	public static final String DATA_DIR = APP_DIR + "/data/facebook";
	
	// PageRank config
	public static final double PAGERANK_CONVERGENCE_TOLERANCE = 0.01; // smaller is more accurate
	public static final double PAGERANK_RESET_PROBABILITY = 0.15;     // probability that the random surfer will not continue to visit other nodes 
}
