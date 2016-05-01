package com.source.HDFS;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import com.source.MAPREDUCE.Mapper;
import com.source.MAPREDUCE.Reducer;

public class test {

	/**
	 * @param args
	 * @throws MalformedURLException 
	 * @throws ClassNotFoundException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class exampleClass = Class.forName("com.source.MAPREDUCE.Reducer");
		Object obj = exampleClass.newInstance();
		Reducer map = (Reducer)obj;
		System.out.println(map.reduce("ssdfs"));

	}

}
