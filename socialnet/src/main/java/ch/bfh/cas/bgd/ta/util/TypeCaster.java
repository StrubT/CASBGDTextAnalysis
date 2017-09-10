package ch.bfh.cas.bgd.ta.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.rdd.RDD;

import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class TypeCaster implements Serializable {

	private static final long serialVersionUID = -846926371569651880L;

	// scala types
	public static final ClassTag<Object> SCALA_OBJECT = ClassTag$.MODULE$.apply(Object.class);
	public static final ClassTag<Integer> SCALA_INTEGER = ClassTag$.MODULE$.apply(Integer.class);
	public static final ClassTag<String> SCALA_STRING = ClassTag$.MODULE$.apply(String.class);
	public static final ClassTag<Double> SCALA_DOUBLE = ClassTag$.MODULE$.apply(Double.class);

	/**
	 * use to cast a vertex RDD with generic type JavaRDD<Object> to the real type JavaRDD<Tuple2<Object,T>>
	 * via a map() transformation
	 * where Tuple2 represents Vertex objects with property type T
	 */
	public static class VertexTypeCaster<T> implements Function<Object, Tuple2<Object, T>>, Serializable {
		private static final long serialVersionUID = 1628637493872783094L;

		public Tuple2<Object, T> call(Object v) throws Exception {
			return (Tuple2<Object, T>)v;
		}
	}

	/**
	 * casts a VertexRDD<Tuple2<Object, T>> to a regular JavaPairRDD<Object, T>
	 * where propertyType is a scala constant of type scala.reflect.ClassTag<T>
	 * 
	 * @param vertexRDD
	 * @param propertyType
	 * @return
	 */
	public static <T> JavaPairRDD<Object, T> toJavaPairRDD(VertexRDD<T> vertexRDD, ClassTag<T> propertyType) {
		return new JavaPairRDD<Object, T>((RDD<Tuple2<Object, T>>) vertexRDD, TypeCaster.SCALA_OBJECT, propertyType);
	}

	/**
	 * casts a VertexRDD<T> to a regular JavaRDD<T>
	 * where propertyType is a scala constant of type scala.reflect.ClassTag<T>
	 * 
	 * @param vertexRDD
	 * @param propertyType
	 * @return
	 */
	public static <T> JavaRDD<T> toJavaRDD(VertexRDD<T> vertexRDD, ClassTag<T> propertyType) {
		return new JavaRDD<T>((RDD<T>) vertexRDD, propertyType);
	}
}
