package com.source.MAPREDUCE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.io.FileReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.source.HDFS.Hdfs;
import com.source.HDFS.INameNode;


public class JobTracker implements IJobTracker{
	
	static int lastJobID = 0;
	static ConcurrentHashMap <Integer, String> taskTrackerIpMap = new ConcurrentHashMap<>();
	static ConcurrentHashMap<Integer, JobDetails> jobDetailsMap = new ConcurrentHashMap<>();
	static ConcurrentHashMap <Integer, BlockingQueue<Integer> > blockNumQueue = new ConcurrentHashMap<>();
	static BlockingQueue<JobDetails> jobQueue = new ArrayBlockingQueue<>(1024);
	static BlockingQueue<ReducerTaskDetails> reduceJobQueue = new ArrayBlockingQueue<>(1024);
	static ConcurrentHashMap<Integer, ArrayList<String> > outputMapFilesMap = new ConcurrentHashMap<>();
	static ConcurrentHashMap<Integer, Integer >counter = new ConcurrentHashMap<>();
	static ConcurrentHashMap<Integer, ReducerTaskDetails > reducerDetailsMap = new ConcurrentHashMap<>();
	static ConcurrentHashMap<Integer, BlockingQueue<String> > reducerfiles = new ConcurrentHashMap<>();
	static ConcurrentHashMap<Integer, Integer > reduceCount = new ConcurrentHashMap<>();
	
	//static BlockingQueue<E>
	static String selfIp = null, nameNodeIp = null;
	static int selfPort = 0, nameNodePort = 0, mapTaskId = 0, reduceTaskId = 0;
	static HashMap<Integer, Integer> taskToJobIdMap = new HashMap<>();
	@Override
	public byte[] jobSubmit(byte[] job) throws RemoteException{
		String mapName = null, reducerName = null, inputFile = null, outputFile = null, keyword = null;
		int numReduceTasks = 0;
		
		try {
			MapReduce.JobSubmitResponse.Builder jobSubmitResponse = MapReduce.JobSubmitResponse.newBuilder();
			MapReduce.JobSubmitRequest jobSubmitRequest = MapReduce.JobSubmitRequest.parseFrom(job);
			mapName = jobSubmitRequest.getMapName();
			reducerName = jobSubmitRequest.getReducerName();
			inputFile = jobSubmitRequest.getInputFile();
			outputFile = jobSubmitRequest.getOutputFile();
			keyword = jobSubmitRequest.getKeyword(); 
			numReduceTasks = jobSubmitRequest.getNumReduceTasks();
			
			//communicate with namenode to get the blocknums of the file
			Hdfs.OpenFileRequest.Builder openFilerequest = Hdfs.OpenFileRequest.newBuilder();
			openFilerequest.setForRead(true);
			openFilerequest.setFileName(inputFile);
			
			//establish RMI with namenode
			String nameNode = "nameNode";
			Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
			INameNode compName = (INameNode) registryNameNode.lookup(nameNode);
			Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
			g.setFileName(inputFile);
			byte[] b = compName.openFile(g.build().toByteArray());
			
			Hdfs.OpenFileResponse res = Hdfs.OpenFileResponse.parseFrom(b);
			int status = res.getStatus();
			if(status == 0){
				System.out.println("No such file");
				jobSubmitResponse.setStatus(0);
				return jobSubmitResponse.build().toByteArray(); 
			}
			ArrayList<Integer> blockList = new ArrayList<>();
			BlockingQueue<Integer> blockQueue = new ArrayBlockingQueue<>(102400);
			for(int i:res.getBlockNumsList()){
				blockList.add(i);
				blockQueue.add(i);
			}
			//System.out.println(blockQueue);
			
			//block location request
			Hdfs.BlockLocationRequest.Builder locReq = Hdfs.BlockLocationRequest.newBuilder();
			for(int i=0;i<blockList.size();i++){
				locReq.addBlockNums(blockList.get(i));
			}
			String ip = null;
			int index;
			int port = 0;
			byte[] locRes = compName.getBlockLocations(locReq.build().toByteArray());
			Hdfs.BlockLocationResponse response = Hdfs.BlockLocationResponse.parseFrom(locRes);
			HashMap<Integer, String> blockLocationMap = new HashMap<>();
			for(Hdfs.BlockLocations blockloc: response.getBlockLocationsList()){
				index = blockloc.getBlockNumber();
				for(Hdfs.DataNodeLocation dataloc : blockloc.getLocationsList()){
					ip = dataloc.getIp();
					port = dataloc.getPort();
				}
				
				blockLocationMap.put(index, ip+":"+port);
				
			}
			
			lastJobID++; // new job so increase ID
			JobDetails jobDetailsObj = new JobDetails();
			jobDetailsObj.setJobId(lastJobID);
			jobDetailsObj.setMapName(mapName);
			jobDetailsObj.setReducerName(reducerName);
			jobDetailsObj.setInputFile(inputFile);
			jobDetailsObj.setOutputFile(outputFile);
			jobDetailsObj.setKeyword(keyword);
			jobDetailsObj.setIsComplete(false);
			jobDetailsObj.setTotalmapTasks(blockList.size());
			jobDetailsObj.setTotalReduceTask(numReduceTasks);
			jobDetailsObj.setBlockList(blockList);
			jobDetailsObj.setBlockLocationMap(blockLocationMap);
			jobDetailsMap.put(lastJobID, jobDetailsObj);
			blockNumQueue.put(lastJobID, blockQueue);
			counter.put(lastJobID,blockList.size());
			
			//putting new job in queue
			jobQueue.add(jobDetailsObj);
			
			//creating response
			jobSubmitResponse.setStatus(1);
			jobSubmitResponse.setJobId(lastJobID);

			return jobSubmitResponse.build().toByteArray();
	
		} catch (InvalidProtocolBufferException | NotBoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public byte[] getJobStatus(byte[] status) throws RemoteException {
		MapReduce.JobStatusRequest jobStatusRequest;
		try {
			jobStatusRequest = MapReduce.JobStatusRequest.parseFrom(status);
			int id = jobStatusRequest.getJobId();
			
			MapReduce.JobStatusResponse.Builder jobStatusResponse =
					MapReduce.JobStatusResponse.newBuilder();
			
			jobStatusResponse.setStatus(1);
			jobStatusResponse.setJobDone(jobDetailsMap.get(id).isComplete());
			jobStatusResponse.setTotalMapTasks(jobDetailsMap.get(id).getTotalmapTasks()); 
			jobStatusResponse.setTotalReduceTasks(jobDetailsMap.get(id).getTotalReduceTask());
			jobStatusResponse.setNumMapTasksCompleted(jobDetailsMap.get(id).getMapTaskDone()); 
			//jobStatusResponse.setNumReduceTasksCompleted(jobDetailsMap.get(id).getReduceTaskDone());
			jobStatusResponse.setNumReduceTasksSCompleted(jobDetailsMap.get(id).getReduceTaskDone());
			
			return jobStatusResponse.build().toByteArray();
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public synchronized byte[] heartBeat(byte[] beat) throws RemoteException {
		int tasktrackerId = 0, jobId = 0, taskId = 0, mapSlotsFree = 0, reduceSlotsFree = 0;
		
		try {
			MapReduce.HeartBeatRequest request = MapReduce.HeartBeatRequest.parseFrom(beat);
			tasktrackerId = request.getTaskTrackerId();
			//System.out.println("TT "+ tasktrackerId);
			mapSlotsFree = request.getNumMapSlotsFree();  
			reduceSlotsFree = request.getNumReduceSlotsFree(); 
			//System.out.println(mapSlotsFree+" "+reduceSlotsFree);
			//System.out.println(request.getMapStatusList().size());
			for(MapReduce.MapTaskStatus status : request.getMapStatusList()){
				jobId = status.getJobId();
				int c = counter.get(jobId) - 1;
				counter.put(jobId, c);
				//System.out.println("out jobid is "+jobId+" "+status.getMapOutputFile());
				if(outputMapFilesMap.containsKey(jobId)){
					outputMapFilesMap.get(jobId).add(status.getMapOutputFile());
				}
				else{
					//System.out.println("ghusa");
					ArrayList<String> temporary = new ArrayList<>();
					temporary.add(status.getMapOutputFile());
					outputMapFilesMap.put(jobId, temporary);
				}
				//System.out.println("value of c is "+c+" "+outputMapFilesMap.get(jobId));
				jobDetailsMap.get(jobId).setMapTaskDone(jobDetailsMap.get(jobId).getTotalmapTasks() - c);  //do it afterw
				if( c == 0){
					//System.out.println("job id "+jobId + "completed");
					ReducerTaskDetails redDetails = new ReducerTaskDetails();
					redDetails.setJobId(jobId);
					//redDetails.setTaskId(getReduceTaskId());
					redDetails.setIsCompleted(false);
					ArrayList<String> outFileList = outputMapFilesMap.get(jobId);
					//System.out.println("size is " + outFileList.size());
					int size = outputMapFilesMap.get(jobId).size();
					ArrayList<String> temp = new ArrayList<>();
					BlockingQueue<String> temp1 = new ArrayBlockingQueue<>(102400);
					for(int i=0;i<size;i++){
						temp.add(outFileList.get(i));
						temp1.add(outFileList.get(i));
					}
					redDetails.setNumReducers(jobDetailsMap.get(jobId).getTotalReduceTask());
					redDetails.setMapOutputFiles(temp);
					
					reduceJobQueue.add(redDetails);
					reducerDetailsMap.put(jobId,redDetails);
					reducerfiles.put(jobId, temp1);
					//System.out.println(temp);
					//System.out.println(temp1);
					reduceCount.put(jobId, jobDetailsMap.get(jobId).getTotalReduceTask());
				}
			}
			
			
			for(MapReduce.ReduceTaskStatus status : request.getReduceStatusList()){
				int id = status.getJobId();
				int c = reduceCount.get(id) - 1;
				reduceCount.put(id, c);
				jobDetailsMap.get(id).setReduceTaskDone(jobDetailsMap.get(id).getTotalReduceTask() - c);
				if(c == 0){
					//System.out.println("ghusa ");
					jobDetailsMap.get(id).setIsComplete(true);
					reducerDetailsMap.get(id).setIsCompleted(true);
				}
				
			}
			
			
			
			MapReduce.HeartBeatResponse.Builder response = MapReduce.HeartBeatResponse.newBuilder();
			response.setStatus(1);
			//System.out.println("JobQueue size "+ jobQueue.size());
			for(int i = 0; i < mapSlotsFree; i++){
				//System.out.println("value of i " + i);
				if(!jobQueue.isEmpty()){
					jobId = jobQueue.peek().getJobId();
					MapReduce.MapTaskInfo.Builder info = MapReduce.MapTaskInfo.newBuilder();
					info.setJobId(jobId);
					info.setTaskId(getMapTaskId());
					info.setMapName(jobDetailsMap.get(jobId).getMapName());
					info.setKeyword(jobDetailsMap.get(jobId).getKeyword());
					int index = blockNumQueue.get(jobId).remove();
				
					String nameNode = "nameNode";
					Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
					INameNode compName = (INameNode) registryNameNode.lookup(nameNode);
					Hdfs.BlockLocationRequest.Builder locReq = 
							Hdfs.BlockLocationRequest.newBuilder();
					locReq.addBlockNums(index);
					String ip = null;
					int port = 0;
					byte[] locRes = compName.getBlockLocations(locReq.build().toByteArray());
					Hdfs.BlockLocationResponse blockLocResponse = Hdfs.BlockLocationResponse.parseFrom(locRes);
					for(Hdfs.BlockLocations blockloc: blockLocResponse.getBlockLocationsList()){
						for(Hdfs.DataNodeLocation dataloc : blockloc.getLocationsList()){
							ip = dataloc.getIp();
							port = dataloc.getPort();
						}
					}
					//System.out.println(index + " ip "+ip);
					MapReduce.DataNodeLocation.Builder dataNodeloc = 
							MapReduce.DataNodeLocation.newBuilder();
					dataNodeloc.setIp(ip);
					dataNodeloc.setPort(port);
					MapReduce.BlockLocations.Builder blockLoc = 
							MapReduce.BlockLocations.newBuilder();
					blockLoc.setBlockNumber(index);
					blockLoc.addLocations(dataNodeloc);
									
					info.addInputBlocks(blockLoc);
					response.addMapTasks(info);
					//System.out.println(blockNumQueue.get(jobId));
					//System.out.println("nikla");
					if(blockNumQueue.get(jobId).isEmpty()){
						//System.out.println("ghusa");
						jobQueue.remove();
					}
				
				}
				
			}
			
//			int x = reduceJobQueue.peek().getNumreducers();
//			if(reduceSlotsFree > x){
//				reduceSlotsFree = x;
//			}
			
			for(int i = 0; i < reduceSlotsFree; i++){
				if(!reduceJobQueue.isEmpty()){
					int div = 0;
					int id = reduceJobQueue.peek().getJobId();
					int count = reducerDetailsMap.get(id).getNumreducers();
					reducerDetailsMap.get(id).setTaskId(getReduceTaskId());
					div = reducerfiles.get(id).size() / count;
					if(div == 0){
						//reducerDetailsMap.get(id).setNumReducers(reducerfiles.get(id).size());
						//jobDetailsMap.get(id).setTotalReduceTask(reducerfiles.get(id).size());
						count = reducerfiles.get(id).size();
						jobDetailsMap.get(id).setTotalReduceTask(count);
						div = reducerfiles.get(id).size() / count;
						reduceCount.put(id, count);
					}
					//System.out.println("vale of div " + div);
					//System.out.println(count + " " + reducerfiles.get(id));
					count--;
					MapReduce.ReducerTaskInfo.Builder reduceTaskInfo = 
							MapReduce.ReducerTaskInfo.newBuilder();
					reduceTaskInfo.setJobId(id);
					reduceTaskInfo.setTaskId(reducerDetailsMap.get(id).getTaskId());
					reduceTaskInfo.setReducerName(jobDetailsMap.get(id).getReducerName());
					reduceTaskInfo.setOutputFile(jobDetailsMap.get(id).getOutputFile());
					
					for(int j = 0; j < div; j++){
						reduceTaskInfo.addMapOutputFiles(reducerfiles.get(id).remove());
					}
												
					reducerDetailsMap.get(id).setNumReducers(count);
					
					response.addReduceTasks(reduceTaskInfo);
					
					if(count == 0){
						reduceJobQueue.remove();
					}
				}
			}
			return  response.build().toByteArray();
		
			
		} catch (InvalidProtocolBufferException | NotBoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String args[]){
		try {
			
			//Reading jobtracker config file
			BufferedReader br = new BufferedReader(new FileReader("JTconfig"));
			String s = br.readLine();
			String str[] = s.split(" ");
			String str1[] = str[0].split(":");
			selfIp = str1[0];
			selfPort = Integer.parseInt(str1[1].trim());
			String str2[] = str[1].split(":");
			nameNodeIp = str2[0];
			nameNodePort = Integer.parseInt(str2[1].trim());
//			while((s=br.readLine()) != null){
//				String temp[] = s.split(" ");
//				taskTrackerIpMap.put(Integer.parseInt(temp[0]), temp[1]);
//			}
			br.close();
			
			br = new BufferedReader(new FileReader("lastids"));
			String ar;
			if((ar=br.readLine()) != null){
				String arr[] = ar.split(" ");
				mapTaskId = Integer.parseInt(arr[0].trim());
				reduceTaskId = Integer.parseInt(arr[1].trim());
				lastJobID = Integer.parseInt(arr[2].trim());
			}
			br.close();
			
			Thread t = new Thread() {
			    public void run() {
			    	JobTracker jt = new JobTracker();
			        while(true){
			        	try {
							jt.update(mapTaskId, reduceTaskId, lastJobID);
							Thread.sleep(500);
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
			        	
			        }
			    }
			};
			t.start();			
			
			//creating jobtracker registery
            String name = "jobtracker";
            java.rmi.registry.LocateRegistry.createRegistry(selfPort);
            IJobTracker engine = new JobTracker();
            IJobTracker stub =
                (IJobTracker) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry(selfIp, selfPort);  // Give IP address of hosting server and the rmi port on which binding is to be done!
            registry.rebind(name, stub);
            System.out.println("Job Tracker bound");
        } catch (Exception e) {
            System.err.println("JobTracker exception:");
            e.printStackTrace();
        }
		  
		
	}
	
	public synchronized int getMapTaskId(){
		mapTaskId++;
		return mapTaskId;
	}
	
	public synchronized int getReduceTaskId(){
		reduceTaskId++;
		return reduceTaskId;
	}
	
	public synchronized void update(int mapId, int reduceId, int jobId) throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter("lastids"));
		out.write(mapTaskId + " " + reduceTaskId + " " + lastJobID);
		out.close();
	}

}
