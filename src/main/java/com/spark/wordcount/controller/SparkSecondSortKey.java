package com.spark.wordcount.controller;

import java.io.Serializable;

import scala.math.Ordered;

public class SparkSecondSortKey implements Ordered<SparkSecondSortKey>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3331567347221968728L;
	
	private int first;
	private int second;

	public SparkSecondSortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean $greater(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.first>other.getFirst()){
			return true;
		}else if(this.first == other.getFirst() && this.second>other.getSecond()) {
			return true;
		}
			return false;
		
	}

	@Override
	public boolean $greater$eq(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.$greater(other)) {
			return true;
		}if(this.first == other.getFirst() && this.second == other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.first < other.getFirst()) {
			return true;
		}else if(this.first < other.getFirst() && this.second <other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.$less(other)) {
			return true;
		}else if(this.getFirst() == other.getFirst() && this.getSecond() == other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.getFirst() - other.getFirst() != 0) {
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		}
		//return 0;
	}

	@Override
	public int compareTo(SparkSecondSortKey other) {
		// TODO Auto-generated method stub
		if(this.getFirst() - other.getFirst() != 0) {
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		}
		//return 0;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SparkSecondSortKey other = (SparkSecondSortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	

}
