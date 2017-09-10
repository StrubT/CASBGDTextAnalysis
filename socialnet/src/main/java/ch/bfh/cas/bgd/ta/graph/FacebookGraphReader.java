package ch.bfh.cas.bgd.ta.graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import ch.bfh.cas.bgd.ta.util.TypeCaster;
import scala.Serializable;
import scala.Tuple2;

public class FacebookGraphReader implements Serializable {
	private static final long serialVersionUID = -4995089017413003138L;

	private static Logger logger = LogManager.getLogger(FacebookGraphReader.class);

	public FacebookGraphReader() {	
	}
	
	public Graph<Double, Integer> readGraph(JavaSparkContext sc, String fileName, long seedNode) throws FileNotFoundException {
		
		logger.info("read graph from " + fileName);
		
		// 1. read all graph data from file
		List<Tuple2<Object, Double>> vertices = new ArrayList<Tuple2<Object, Double>>();
		vertices.add(new Tuple2<Object, Double>(new Long(seedNode), 0.0)); // also add seed node
		
		List<Edge<Integer>> edges = new ArrayList<Edge<Integer>>();
	    File file = new File(fileName);
	    Scanner scanner = new Scanner(file);
	    while(scanner.hasNextLine()) {        
	        String e = scanner.nextLine();
	        String[] nodes = e.split(" ");
	        long node1 = Long.parseLong(nodes[0]);
	        long node2 = Long.parseLong(nodes[1]);
	        
	        // add vertices
			vertices.add(new Tuple2<Object, Double>(new Long(node1), 0.0)); 
			vertices.add(new Tuple2<Object, Double>(new Long(node2), 0.0)); 
	        
	        // graph is not directed, so create edges in both directions
	        edges.add(new Edge<Integer>(node1, node2, 0));
	        edges.add(new Edge<Integer>(node2, node1, 0));
	        
	        // also add connections to seed node not given explicitly in the file
//	        edges.add(new Edge<Integer>(seedNode, node1, 0));
//	        edges.add(new Edge<Integer>(node1, seedNode, 0));
//	        edges.add(new Edge<Integer>(seedNode, node2, 0));
//	        edges.add(new Edge<Integer>(node2, seedNode, 0));	        
	    } 
	    scanner.close();

	    JavaRDD<Tuple2<Object, Double>> verticesRDD = sc.parallelize(vertices);
	    verticesRDD.distinct(); // remove duplicates
//	    verticesRDD.saveAsTextFile("vertices");
	    
		JavaRDD<Edge<Integer>> edgesRDD = sc.parallelize(edges);
		edgesRDD.distinct(); // in case we have duplicates
//		edgesRDD.saveAsTextFile("edges");
		
		// 2. create graph
		Graph<Double, Integer> graph = Graph.apply(verticesRDD.rdd(), 
				edgesRDD.rdd(),
				0.0, 
				StorageLevel.MEMORY_ONLY(), 
				StorageLevel.MEMORY_ONLY(),
				TypeCaster.SCALA_DOUBLE,
				TypeCaster.SCALA_INTEGER);
		
		logger.info("graph has " + graph.vertices().count() + " vertices and " + graph.edges().count() + " edges");
		
		return graph;
	}
}
