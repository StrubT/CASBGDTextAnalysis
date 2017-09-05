package ch.bfh.cas.bgd.ta.util;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import scala.Serializable;

public class Review implements Serializable {
	private static final long serialVersionUID = 3796733404117144398L;

	private String id = null;
	private String review = null;
	private double classGS = Configuration.SCORE_NEUTRAL;
	private double classSA = Configuration.SCORE_NEUTRAL;

	public Review() {
	}
	
	public Review(String id, String review, double classGS) {
		this.id = id;
		this.review = review;
		this.classGS = classGS;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getReview() {
		return review;
	}
	
	public void setReview(String review) {
		this.review = review;
	}
	
	public double getClassGS() {
		return classGS;
	}
	
	public void setClassGS(double classGS) {
		this.classGS = classGS;
	}
	
	public double getClassSA() {
		return classSA;
	}
	
	public void setClassSA(double classSA) {
		this.classSA = classSA;
	}
}
