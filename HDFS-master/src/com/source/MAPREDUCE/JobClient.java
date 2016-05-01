package com.source.MAPREDUCE;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class JobClient {
	public static void main(String args[]) throws InterruptedException{
		try {
			//reading JT config files
			BufferedReader br = new BufferedReader(new FileReader("JCconfig"));
			String inputfile = "input.txt";  //replace with args[]
			String outputfile = "keen"; //replace with args[]
			String keyword = "head";
			int numreducetask = 3;
			String s = br.readLine();
			String str[] = s.split(" ");
			String str1[] = str[0].split(":");
			String JTip = str1[0];
			String mappername = str[1];
			String reducername = str[2];
			int JTport = Integer.parseInt(str1[1]);
			br.close();
			
			//building jobsubmit request
			MapReduce.JobSubmitRequest.Builder jobsubmitrequest = 
					MapReduce.JobSubmitRequest.newBuilder();
			jobsubmitrequest.setMapName(mappername);		
			jobsubmitrequest.setReducerName(reducername);
			jobsubmitrequest.setInputFile(inputfile);
			jobsubmitrequest.setOutputFile(outputfile);
			jobsubmitrequest.setKeyword(keyword);
			jobsubmitrequest.setNumReduceTasks(numreducetask);
			
			//RMI with Jobtracker
			String JT = "jobtracker";
            Registry registryJobTracker = 
            		LocateRegistry.getRegistry(JTip, JTport);
            IJobTracker tracker = 
            		(IJobTracker) registryJobTracker.lookup(JT);
            
            //sending request to jobtracker
            byte[] jobTrackerResponse = 
            		tracker.jobSubmit(jobsubmitrequest.build().toByteArray());
            MapReduce.JobSubmitResponse jobSubmitResponse =
            		MapReduce.JobSubmitResponse.parseFrom(jobTrackerResponse);
            
            int status = jobSubmitResponse.getStatus();
            int jobId = jobSubmitResponse.getJobId();
            //System.out.println(jobId);
            if(status == 0){
            	System.out.println("Error");
            	System.exit(0);
            }
            //client will wait in while loop till job is done
            MapReduce.JobStatusRequest.Builder jobStatus = MapReduce.JobStatusRequest.newBuilder();
            jobStatus.setJobId(jobId);
            while(true){
            	byte[] response = tracker.getJobStatus(jobStatus.build().toByteArray());
            	MapReduce.JobStatusResponse jobStatusResponse = 
            			MapReduce.JobStatusResponse.parseFrom(response);
            	            	
            	boolean isDone = jobStatusResponse.getJobDone();
            	int totalMapTasks = jobStatusResponse.getTotalMapTasks();
            	int totalReduceTasks = jobStatusResponse.getTotalReduceTasks();
            	int mapTaskCompleted = jobStatusResponse.getNumMapTasksCompleted();
            	int reduceTaskCompleted = jobStatusResponse.getNumReduceTasksSCompleted();
            	
            	
            	System.err.println("Map("+ (float)mapTaskCompleted * 100/(float)totalMapTasks
            			+"%) Reduce(" + (float)reduceTaskCompleted * 100/(float)totalReduceTasks+"%)");
            	//System.out.println(isDone);
            	//System.out.println();
            	if(isDone){
            		System.out.println("Done");
            		break;
            	}
            	Thread.sleep(3000);
            	
            	
            	
            }
            
            
			
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
