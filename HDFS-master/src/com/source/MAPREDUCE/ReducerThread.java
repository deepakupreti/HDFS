package com.source.MAPREDUCE;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.ByteString;
import com.source.HDFS.Hdfs;
import com.source.HDFS.IDataNode;
import com.source.HDFS.INameNode;

public class ReducerThread implements Runnable {

	private BlockingQueue<ReducerTaskDetails> completedReducerTaskQueue;
	private ReducerTaskDetails taskDetails;
	public ReducerThread(){}
    public ReducerThread(ReducerTaskDetails taskDetails, 
    		BlockingQueue<ReducerTaskDetails> completedReducerTaskQueue){
    	
    	this.completedReducerTaskQueue = completedReducerTaskQueue;
    	this.taskDetails = taskDetails;
    }
        
	@Override
	public void run() {//jobid,taskid,datanodeip,datanodeport,outputpath,index,keyword,namenodeip,blocksize
		INameNode compName = null ;
		IDataNode compData = null;
		String nameNode = "nameNode";
		String dataNode = "dataNode";
		String reduceName = null;
		try{
			int jobId = taskDetails.getJobId();
			int taskId = taskDetails.getTaskId();
			//System.out.println("got id "+jobId+" "+taskId);
			String nameNodeIp = taskDetails.getNameNodeIp();
			String outputfile = taskDetails.getOutputFile();
			String outputPath = taskDetails.getOutputPath();
			int blockSize = taskDetails.getBlockSize();
			reduceName = taskDetails.getReducerName();
			//String fileName = null;
			String name = outputfile+"_" + jobId + "_" + taskId;
			PrintWriter out = new PrintWriter(outputPath + name);
			ArrayList<String> mapperFiles = taskDetails.getMapOutputFiles();
			for(String fileName : mapperFiles){
				//String nameNode = "nameNode";
				//INameNode compName = null;
				Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
				compName = (INameNode) registryNameNode.lookup(nameNode);
				Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
				g.setFileName(fileName);
				byte[] b = compName.openFile(g.build().toByteArray());
				
				Hdfs.OpenFileResponse res = Hdfs.OpenFileResponse.parseFrom(b);
				int status = res.getStatus();
				if(status == 0){
					System.out.println("No such file");
					return ;
				}
				ArrayList<Integer> blockList = new ArrayList<>();
				for(int i:res.getBlockNumsList()){
					blockList.add(i);
				}
				Hdfs.BlockLocationRequest.Builder locReq = Hdfs.BlockLocationRequest.newBuilder();
				for(int i=0;i<blockList.size();i++){
					locReq.addBlockNums(blockList.get(i));
				}
				String ip = null;
				int index;
				int port = 0;
				byte[] locRes = compName.getBlockLocations(locReq.build().toByteArray());
				Hdfs.BlockLocationResponse response = Hdfs.BlockLocationResponse.parseFrom(locRes);
				
				for(Hdfs.BlockLocations blockloc: response.getBlockLocationsList()){
					index = blockloc.getBlockNumber();
					for(Hdfs.DataNodeLocation dataloc : blockloc.getLocationsList()){
						ip = dataloc.getIp();
						port = dataloc.getPort();
					}
					//String dataNode = "dataNode";
		            Registry registryDataNode = LocateRegistry.getRegistry(ip, port);
		            compData = (IDataNode) registryDataNode.lookup(dataNode);
		            Hdfs.ReadBlockRequest.Builder readReq = Hdfs.ReadBlockRequest.newBuilder();
		            readReq.setBlockNumber(index);
		            byte[] chunk = compData.readBlock(readReq.build().toByteArray());
					Hdfs.ReadBlockResponse readresponse = Hdfs.ReadBlockResponse.parseFrom(chunk);
					for(ByteString val : readresponse.getDataList()){
						String s = new String(val.toByteArray());
						Class exampleClass = Class.forName("com.source.MAPREDUCE."+reduceName);
						Object obj = exampleClass.newInstance();
						Reducer r = (Reducer)obj;
						//System.out.println(map.reduce("ssdfs"));
						//Reducer r = new Reducer();
						out.write(r.reduce(s));
					}
				}
			}
			out.close();
			
			//put it back in hdfs file system
			Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
			g.setFileName(name);
			//INameNode compName = null ;
			//String nameNode = "nameNode";
            Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
            compName = (INameNode) registryNameNode.lookup(nameNode);
            byte[] openfile = compName.openFile(g.build().toByteArray());
            Hdfs.OpenFileResponse res1 = Hdfs.OpenFileResponse.parseFrom(openfile);
            int status = res1.getStatus();
            //System.out.println("status "+ status); // 0 mea
            if(status==1){
            	System.out.println("File Already Present");
            	return;
            }
            File file = new File(outputPath + name);
			InputStream in = new FileInputStream(file);
			int readBytes = 0;
			byte[] by = new byte[blockSize];
			byte[] arr;
			while ((readBytes  = in.read(by)) != -1) {
		    	arr = Arrays.copyOfRange(by, 0, readBytes);
		        byte b[] = compName.assignBlock(g.build().toByteArray());
		        Hdfs.AssignBlockResponse res = Hdfs.AssignBlockResponse.parseFrom( b );
		        Hdfs.BlockLocations loc = res.getNewBlock();
		        int index = loc.getBlockNumber();
		        String ips[] = new String[4];
		        int port = 0;
		        int i = 0;
		        for (Hdfs.DataNodeLocation dat: loc.getLocationsList()){
		        	ips[i++] = dat.getIp();
		        	port = dat.getPort();
		        }
		        		        
		        String ip1 = ips[0];
		        String ip2 = ips[1];
		        //String dataNode = "dataNode";
	            Registry registryDataNode = LocateRegistry.getRegistry(ip1, port);
	            compData = (IDataNode) registryDataNode.lookup(dataNode);
	            
	            Hdfs.BlockLocations.Builder g1 = Hdfs.BlockLocations.newBuilder();
	            Hdfs.DataNodeLocation.Builder location = Hdfs.DataNodeLocation.newBuilder();
	            location.setIp(ip2);
	            //location.setPort(15000);
	            
	            g1.setBlockNumber(index);
	            g1.addLocations(location);
	            Hdfs.WriteBlockRequest.Builder g2 = Hdfs.WriteBlockRequest.newBuilder();
	            g2.setBlockInfo(g1);
	            g2.addData(ByteString.copyFrom(arr));
	            byte[] response = compData.writeBlock(g2.build().toByteArray());
	            Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
	            compName.closeFile(closeReq.build().toByteArray());
	            

		    }
			taskDetails.setOutputFile(name);
			taskDetails.setIsCompleted(true);
			completedReducerTaskQueue.add(taskDetails);
		}
		catch(Exception e){
			System.err.println("Exception:");
            Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
            try {
				compName.closeFile(closeReq.build().toByteArray());
			} catch (RemoteException e1) {
				e1.printStackTrace();
			}
            e.printStackTrace();
            
		}
	}
}
