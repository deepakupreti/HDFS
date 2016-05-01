package com.source.MAPREDUCE;

public class TasksDetails {
	private int jobId = 0;
	private int taskId = 0;
	private boolean isCompleted = false;
	private String outputFile = null;
	
	private String dataNodeIp = null;
	private int dataNodePort = 0;
	private String outputpath = null;
	private int index = 0;
	private String namnodeIp = null;
	private int blockSize = 0;
	private String keyword = null;
	private String mapperName = null;
	
	
	void setJobId(int jobId){
		this.jobId = jobId;
	}
	
	void setTaskId(int taskId){
		this.taskId = taskId;
	}
	
	void setIsCompleted(boolean isCompleted){
		this.isCompleted = isCompleted;
	}
	
	void setMapOutputFile(String outputFile){
		this.outputFile = outputFile;
	}
	
	void setDataNodeIp(String dataNodeIp){
		this.dataNodeIp = dataNodeIp;
	}
	
	void setdataNodePort(int dataNodePort){
		this.dataNodePort = dataNodePort;
	}
	
	void setOutPutPath(String outputPath){
		this.outputpath = outputPath;
	}
	
	void setIndex(int index){
		this.index = index;
	}
	
	void setNameNodeIp(String nameNodeIp){
		this.namnodeIp = nameNodeIp;
	}
	
	void setblockSize(int blockSize){
		this.blockSize = blockSize;
	}
	
	void setKeyword(String keyword){
		this.keyword = keyword;
	}
	
	void setMapperName(String mapperName){
		this.mapperName = mapperName;
	}
	
	int getJobId(){
		return jobId;
	}
	
	int getTaskId(){
		return taskId;
	}
	
	boolean getIsCompleted(){
		return isCompleted;
	}
	
	String getMapOutputFile(){
		return outputFile;
	}
	
	
	String getDataNodeIp(){
		return dataNodeIp;
	}
	
	int getdataNodePort(){
		return dataNodePort;
	}
	
	String getOutPutPath(){
		return outputpath;
	}
	
	int getIndex(){
		return index;
	}
	
	String getNameNodeIp(){
		return namnodeIp;
	}
	
	int getblockSize(){
		return blockSize;
	}
	
	String getKeyword(){
		return keyword;
	}
	
	String getMapperName(){
		return mapperName; 
	}
	
	
}

