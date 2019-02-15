package com.spark.wordcount.controller;

import java.io.Serializable;

public class Students implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String name;
	Integer age;
	Integer id;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
}
