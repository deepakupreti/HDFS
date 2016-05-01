package com.source.MAPREDUCE;

import java.util.ArrayList;

public class ReducerTaskDetails {
	private int jobId = 0;
	private int taskId = 0;
	private boolean isCompleted = false;
	private String outputFile = null;
	private int numReduce;
	private String namnodeIp = null;
	private ArrayList<String> mapOutputFiles;
	private String outputPath = null;
	private String reducerName = null;
	private int blockSize = 0;
	
	void setJobId(int jobId){
		this.jobId = jobId;
	}
	
	void setTaskId(int taskId){
		this.taskId = taskId;
	}
	
	void setIsCompleted(boolean isCompleted){
		this.isCompleted = isCompleted;
	}
	
	void setNumReducers(int numreduce){
		this.numReduce = numreduce;
	}
	
	void setMapOutputFiles(ArrayList<String> list){
		this.mapOutputFiles = list;
	}
	
	void setNameNodeIp(String nameNodeIp){
		this.namnodeIp = nameNodeIp;
	}
	
	void setOutputFile(String outputFile){
		this.outputFile = outputFile;
	}
	
	void setOutputPath(String outputPath){
		this.outputPath = outputPath;
	}
	
	void setReducerName(String reducerName){
		this.reducerName = reducerName;
	}
	
	void setBlockSize(int blockSize){
		this.blockSize = blockSize;
	}

//	void setDataNodeIp(String dataNodeIp){
//		this.dataNodeIp = dataNodeIp;
//	}
//	

	
//	void setOutPutPath(String outputPath){
//		this.outputpath = outputPath;
//	}
//	
//	void setIndex(int index){
//		this.index = index;
//	}
//	
//	void setNameNodeIp(String nameNodeIp){
//		this.namnodeIp = nameNodeIp;
//	}
//	
//	void setblockSize(int blockSize){
//		this.blockSize = blockSize;
//	}
//	

	
	int getJobId(){
		return jobId;
	}
	
	int getTaskId(){
		return taskId;
	}
	
	boolean getIsCompleted(){
		return isCompleted;
	}
	
	int getNumreducers(){
		return numReduce;
	}
	
	ArrayList<String> getMapOutputFiles(){
		return mapOutputFiles;
	}
	
	String getNameNodeIp(){
		return namnodeIp;
	}
	
	String getOutputFile(){
		return outputFile;
	}
	
	String getOutputPath(){
		return outputPath;
	}
	
	String getReducerName(){
		return reducerName;
	}
	
	int getBlockSize(){
		return blockSize;
	}

}
