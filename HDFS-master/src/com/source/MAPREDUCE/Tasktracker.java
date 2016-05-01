package com.source.MAPREDUCE;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.source.HDFS.INameNode;
import com.source.HDFS.NameNode;


public class Tasktracker implements ITasktracker{
	static int id = 0, selfPort = 0, jobTrackerPort = 0;
	static String selfIp = null, jobTrackerIp = null;
	
	static BlockingQueue<TasksDetails> completedMapTaskQueue = new ArrayBlockingQueue<>(102400);
	static BlockingQueue<ReducerTaskDetails> completedReduceTaskQueue = new ArrayBlockingQueue<>(102400);
	static BlockingQueue<TasksDetails> currentMapTaskQueue = new ArrayBlockingQueue<>(102400);
	static BlockingQueue<ReducerTaskDetails> currentReduceTaskQueue = new ArrayBlockingQueue<>(102400);
	static int taskId = 0, blockSize = 0;
	static String OUTPUT = null;
	//static String keyword = null;
	static String namenodeip = null;
	ThreadPoolExecutor mapExecutor;
	ThreadPoolExecutor reduceExecutor;
	//static BlockingQueue<TasksDetails> mapFlagQueue = new ArrayBlockingQueue<>(102400);
	public static void main(String args[]) throws RemoteException {
		try {
			//read config files
			BufferedReader br = new BufferedReader(new FileReader("TTconfig"));
			String str[] = br.readLine().split(" ");
			id = Integer.parseInt(str[0].trim());
			String str1[] = str[1].split(":");
			selfIp = str1[0];
			selfPort = Integer.parseInt(str1[1]);
			String str2[] = str[2].split(":");
			jobTrackerIp = str2[0];
			jobTrackerPort = Integer.parseInt(str2[1]);
			OUTPUT = str[3];
			namenodeip = str[4];
			blockSize = Integer.parseInt(str[5]);
			br.close();
			//br = new BufferedReader(new FileReader("keyword"));
			//keyword = br.readLine().trim();
			//br.close();
			
			
			//creating tasktracker registery
			try {
	            String name = "tasktracker";
	            java.rmi.registry.LocateRegistry.createRegistry(selfPort);
	            ITasktracker engine = new Tasktracker();
	            ITasktracker stub =
	                (ITasktracker) UnicastRemoteObject.exportObject(engine, 0);
	            Registry registry = LocateRegistry.getRegistry(selfIp, selfPort);  // Give IP address of hosting server and the rmi port on which binding is to be done!
	            registry.rebind(name, stub);
	            System.out.println("TaskTracker bound");
	        } catch (Exception e) {
	            System.err.println("TaskTracker exception:");
	            e.printStackTrace();
	        }
			
			Tasktracker t = new Tasktracker();
			t.process();
			
				
			}
		 catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void process() throws RemoteException {
		//map pool
		mapExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		mapExecutor.setMaximumPoolSize(10);
		
		//reduce pool
		reduceExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		reduceExecutor.setMaximumPoolSize(10);
		
		
		while(true){
						
			MapReduce.HeartBeatRequest.Builder heartBeatRequest = 
					MapReduce.HeartBeatRequest.newBuilder();
			
			heartBeatRequest.setTaskTrackerId(id);
			heartBeatRequest.setNumMapSlotsFree(
					mapExecutor.getMaximumPoolSize() - mapExecutor.getActiveCount());
			heartBeatRequest.setNumReduceSlotsFree(
					reduceExecutor.getMaximumPoolSize() - reduceExecutor.getActiveCount());
			
			for(TasksDetails details : completedMapTaskQueue){
				MapReduce.MapTaskStatus.Builder mapTaskStatus = 
						MapReduce.MapTaskStatus.newBuilder();
				mapTaskStatus.setJobId(details.getJobId());
				mapTaskStatus.setTaskId(details.getTaskId());
				mapTaskStatus.setTaskCompleted(details.getIsCompleted());
				mapTaskStatus.setMapOutputFile(details.getMapOutputFile());
				heartBeatRequest.addMapStatus(mapTaskStatus);
				
			}
			
			for(ReducerTaskDetails details : completedReduceTaskQueue){
				//System.out.println("came back "+details.getTaskId());
				MapReduce.ReduceTaskStatus.Builder reducerTaskStatus =
						MapReduce.ReduceTaskStatus.newBuilder();
				reducerTaskStatus.setJobId(details.getJobId());
				reducerTaskStatus.setTaskId(details.getTaskId());
				reducerTaskStatus.setTaskCompleted(details.getIsCompleted());
				heartBeatRequest.addReduceStatus(reducerTaskStatus);
				
			}
					
			completedMapTaskQueue.clear();
			completedReduceTaskQueue.clear();
			
							
			//heartBeatRequest.ad
			String jobtracker = "jobtracker";
			Registry registryJobTracker = LocateRegistry.getRegistry(jobTrackerIp, jobTrackerPort);
			IJobTracker compName = null;
			TasksDetails details = null;
			ReducerTaskDetails reducerDetails = null;
			try {
				compName = (IJobTracker) registryJobTracker.lookup(jobtracker);
				byte[] response = compName.heartBeat(heartBeatRequest.build().toByteArray());
	            MapReduce.HeartBeatResponse heartBeatResponse = 
	            		MapReduce.HeartBeatResponse.parseFrom(response);
	            
	            int status = heartBeatResponse.getStatus();
	            for(MapReduce.MapTaskInfo info : heartBeatResponse.getMapTasksList()){
	            	int jobId = info.getJobId();
	            	int taskId = info.getTaskId();
	            	//System.out.println("tasksids "+taskId);
	            	String mapName = info.getMapName();
	            	String keyword = info.getKeyword();
	            	int port = 0, index = 0;
	            	String ip = null;
	            	for(MapReduce.BlockLocations loc : info.getInputBlocksList()){
	            		index = loc.getBlockNumber();
	            		for(MapReduce.DataNodeLocation dataloc : loc.getLocationsList()){
	            			ip = dataloc.getIp();
	            			port = dataloc.getPort();
	            		}
	            	}
	            	details = new TasksDetails();
	            	details.setJobId(jobId);
	            	details.setTaskId(taskId);
	            	details.setIsCompleted(false);
	            	details.setDataNodeIp(ip);
	            	details.setdataNodePort(port);
	            	details.setOutPutPath(OUTPUT);
	            	details.setIndex(index);
	            	details.setNameNodeIp(namenodeip);
	            	details.setblockSize(blockSize);
	            	details.setKeyword(keyword);
	            	details.setMapperName(mapName);
	            	currentMapTaskQueue.add(details);
	            	
	            }
	            
	            
	            for(MapReduce.ReducerTaskInfo info : heartBeatResponse.getReduceTasksList()){
	            	int jobId = info.getJobId();
	            	int taskId = info.getTaskId();
	            	//System.out.println("tasksids reducer"+taskId);
	            	String reducerName = info.getReducerName();
	            	
	            	
	            	reducerDetails = new ReducerTaskDetails();
	            	reducerDetails.setJobId(jobId);
	            	reducerDetails.setTaskId(taskId);
	            	reducerDetails.setOutputFile(info.getOutputFile());
	            	reducerDetails.setReducerName(info.getReducerName());
	            	ArrayList<String> temp = new ArrayList<>();
	            	for(String s: info.getMapOutputFilesList()){
	            		temp.add(s);
	            	}
	            	reducerDetails.setMapOutputFiles(temp);
	            	reducerDetails.setIsCompleted(false);
	            	reducerDetails.setNameNodeIp(namenodeip);
	            	reducerDetails.setOutputPath(OUTPUT);
	            	reducerDetails.setBlockSize(blockSize);
	            	currentReduceTaskQueue.add(reducerDetails);
	            	
	            }
	            
	            
	           // System.out.println(currentMapTaskQueue.size());
	          //  System.out.println(currentMapTaskQueue);
	            //execute threads
	            while(!currentMapTaskQueue.isEmpty()){
	            	MapperThread mt = 
	            			new MapperThread(currentMapTaskQueue.remove(), completedMapTaskQueue);
	            	
	            	mapExecutor.execute(mt);
	            }
	            
	            while(!currentReduceTaskQueue.isEmpty()){
	            	ReducerThread rt = 
	            			new ReducerThread(currentReduceTaskQueue.remove(), completedReduceTaskQueue);
	            	
	            	reduceExecutor.execute(rt);
	            }
	            
	                        
				Thread.sleep(1000);
			} catch (NotBoundException | InterruptedException | InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			
            //break;
			
		}
	}
	


}
