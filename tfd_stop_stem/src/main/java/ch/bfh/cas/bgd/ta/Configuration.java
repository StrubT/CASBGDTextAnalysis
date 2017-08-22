package ch.bfh.cas.bgd.ta;

import scala.Serializable;

public class Configuration implements Serializable {

	private static final long serialVersionUID = -764801264720134487L;

	// Spark master node URL
	public static final String SPARK_MASTER_URL = "spark://bigdata-VirtualBox:7077";

	// application config
	public static final String APP_NAME = "ch.bfh.cas.ta.tfd_stop_stem";
}
