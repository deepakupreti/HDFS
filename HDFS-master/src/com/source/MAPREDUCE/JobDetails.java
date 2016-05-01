package com.source.MAPREDUCE;

import java.util.ArrayList;
import java.util.HashMap;

public class JobDetails {
	private int id;
	private String mapName;
	private String reducerName;
	private String inputFile;
	private String outputFile;
	//private int numReduceTasks = 0;
	private boolean isComplete = false;
	private int totalMapTasks = 0;
	private int mapTaskDone = 0;
	private int totalReduceTasks = 0;
	private int reduceTasksDone = 0;
	private String keyword = null;
	private ArrayList<Integer> blockList;
	private HashMap<Integer, String> blockLocationMap;
	
	//private ArrayList<String> mapFileList;
	
	void setJobId(int id){
		this.id = id;
	}
	
	void setMapName(String mapName){
		this.mapName = mapName;
	}
	
	void setReducerName(String reducerName){
		this.reducerName = reducerName;
	}
	
	void setInputFile(String inputFile){
		this.inputFile = inputFile;
	}
	
	void setOutputFile(String outputFile){
		this.outputFile  = outputFile;
	}
	
	void setKeyword(String keyword){
		this.keyword  = keyword;
	}
		
	
	void setIsComplete(boolean is){
		this.isComplete = is;
	}
	
	void setTotalmapTasks(int totalMapTasks){
		this.totalMapTasks = totalMapTasks;
	}
	
	void setMapTaskDone(int mapTaskDone){
		this.mapTaskDone = mapTaskDone;
	}
		
	void setTotalReduceTask(int totalReduceTasks){
		this.totalReduceTasks = totalReduceTasks;
	}
	
	void setReduceTaskDone(int reduceTaskDone){
		this.reduceTasksDone = reduceTaskDone;
	}
	
	void setBlockList(ArrayList<Integer> blockList){
		this.blockList = new ArrayList<>();
		for(int i=0; i<blockList.size(); i++){
			this.blockList.add(blockList.get(i));
		}
	}
	
	void setBlockLocationMap(HashMap<Integer, String> blockLocationMap){
		this.blockLocationMap = new HashMap<>();
		blockLocationMap.putAll(blockLocationMap);
	}
	
//	void setMapFileList(ArrayList<Integer> mapFileListList){
//		this.mapFileList = new ArrayList<>();
//		for(int i=0; i<mapFileList.size(); i++){
//			this.mapFileList.add(mapFileList.get(i));
//		}
//	}
	
	int  getJobId(){
		return id;
	}
	
	String getMapName(){
		return mapName;
	}
	
	String getReducerName(){
		return reducerName;
	}
	
	String getInputFile(){
		return inputFile;
	}
	
	String getOutputFile(){
		return outputFile;
	}
	
	String getKeyword(){
		return keyword ;
	}
	
	boolean isComplete(){
		return isComplete;
	}
	
	int getTotalmapTasks(){
		return totalMapTasks;
	}
	
	int getMapTaskDone(){
		return mapTaskDone;
	}
		
	int getTotalReduceTask(){
		return totalReduceTasks;
	}
	
	int getReduceTaskDone(){
		return reduceTasksDone;
	}
	
	
	ArrayList<Integer> getBlockList(){
		return blockList;
	}
	
	HashMap<Integer, String> getBlockLocationMap(){
		return blockLocationMap;
	}
	
//	ArrayList<String> getmapFileList(){
//		return mapFileList;
//	}
	
}
