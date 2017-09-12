
package ch.bfh.cas.bgd.ta.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;

import ch.bfh.cas.bgd.ta.graph.FacebookGraphReader;
import ch.bfh.cas.bgd.ta.util.TypeCaster;
import scala.Serializable;
import scala.Tuple2;

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

	public static void main(String[] args) throws IOException {

		if (args.length != 0) {
			System.out.println("Usage: Application");
			System.exit(0);
		}

		long seedId = 107L;
		FacebookGraphReader graphReader = new FacebookGraphReader();
		Graph<Double, Integer> graph = graphReader.readGraph(Application.getSparkContext(), //
			String.format("%s/%d.edges", Configuration.DATA_DIR, seedId), seedId);

		class MyComparator implements Comparator<Tuple2<Long, Integer>>, Serializable {

			private static final long serialVersionUID = 5796280812892748301L;

			@Override
			public int compare(Tuple2<Long, Integer> x, Tuple2<Long, Integer> y) {

				return -Integer.compare(x._2(), y._2());
			}
		}

		List<String> mostFriends = new JavaPairRDD<Long, Integer>((RDD)graph.ops().outDegrees(), TypeCaster.SCALA_LONG, TypeCaster.SCALA_INTEGER) //
			.takeOrdered(5, new MyComparator()).stream() //
			.map(t -> String.format("%d (%d)", t._1(), t._2())) //
			.collect(Collectors.toList());

		class MyPropertyMapper implements Function<Tuple2<Long, Long>, Long>, Serializable {

			private static final long serialVersionUID = 7117762102816550697L;

			@Override
			public Long call(Tuple2<Long, Long> tuple) throws Exception {

				return tuple._2();
			}
		}

		long numComponents = new JavaPairRDD<Long, Long>((RDD)graph.ops().connectedComponents().vertices(), TypeCaster.SCALA_LONG, TypeCaster.SCALA_LONG) //
			.map(new MyPropertyMapper()) //
			.distinct(1).count();

		Path resultPath = Paths.get("result.txt");
		Files.write(resultPath, Arrays.asList("TOP 5 MOST CONNECTED FRIENDS"));
		Files.write(resultPath, mostFriends, StandardOpenOption.APPEND);
		Files.write(resultPath, Arrays.asList("", String.format("# components: %d", numComponents)), StandardOpenOption.APPEND);
	}
}
