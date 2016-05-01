package com.source.MAPREDUCE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.ByteString;
import com.source.HDFS.Hdfs;
import com.source.HDFS.IDataNode;
import com.source.HDFS.INameNode;

public class MapperThread implements Runnable {

	private BlockingQueue<TasksDetails> completedMapTaskQueue;
	private TasksDetails taskDetails;
	public MapperThread(){}
    public MapperThread(TasksDetails taskDetails, BlockingQueue<TasksDetails> completedMapTaskQueue){
    	this.completedMapTaskQueue = completedMapTaskQueue;
    	this.taskDetails = taskDetails;
    }
        
	@Override
	public void run() {//jobid,taskid,datanodeip,datanodeport,outputpath,index,keyword,namenodeip,blocksize
		try{
			//String stringArray[] = name.split(",");
			int jobId = taskDetails.getJobId();
			int taskId = taskDetails.getTaskId();
			String dataNodeIp = taskDetails.getDataNodeIp();
			int datanodePort = taskDetails.getdataNodePort();
			String OUTPUT = taskDetails.getOutPutPath();
			int index = taskDetails.getIndex();
			String keyword = taskDetails.getKeyword();
			String namenodeIp = taskDetails.getNameNodeIp();
			int blockSize = taskDetails.getblockSize();
			String mapperName = taskDetails.getMapperName();
			StringBuilder sb = new StringBuilder();
			
			//connection with datanode
			String dataNode = "dataNode";
	        Registry registryDataNode = LocateRegistry.getRegistry(dataNodeIp, datanodePort);
	        IDataNode compData = (IDataNode) registryDataNode.lookup(dataNode);
	        Hdfs.ReadBlockRequest.Builder readReq = Hdfs.ReadBlockRequest.newBuilder();
	        readReq.setBlockNumber(index);
	        byte[] chunk = compData.readBlock(readReq.build().toByteArray());
			Hdfs.ReadBlockResponse readresponse = Hdfs.ReadBlockResponse.parseFrom(chunk);
			for(ByteString val : readresponse.getDataList()){
				String s = new String(val.toByteArray());
				sb.append(s);
			}
			//System.out.println(sb.toString());
			String str[] = sb.toString().split("\n");
			
			Class exampleClass = Class.forName("com.source.MAPREDUCE."+mapperName);
			Object obj = exampleClass.newInstance();
			Mapper map = (Mapper)obj;
			String val = null;
			String filename = jobId+"_map_" + taskId;
			PrintWriter out = new PrintWriter(OUTPUT + filename);
			//FileOutputStream out = new FileOutputStream(new File(OUTPUT + filename));
			//System.out.println("Length of string is "+str.length);
			for(int i=0;i<str.length;i++){
				//System.out.println(taskId+" "+jobId+" "+keyword+" "+str[i]);
				val = map.map(keyword+"<-@->"+str[i]);
				if(!val.equals("null")){
					out.write(str[i]);
					out.write("\n");
				}
				else{
					out.write("");
				}
			}
			out.close();
			BufferedReader check = new BufferedReader(new FileReader(OUTPUT + filename));     
			if (check.readLine() == null) {
				PrintWriter setflag = new PrintWriter(OUTPUT + filename);
				setflag.write(" ");
				setflag.close();
				
			}
			//file put request
			Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
			g.setFileName(filename);
			
			INameNode compName = null ;
			try {
	            String nameNode = "nameNode";
	            Registry registryNameNode = LocateRegistry.getRegistry(namenodeIp);
	            compName = (INameNode) registryNameNode.lookup(nameNode);
	            byte[] openfile = compName.openFile(g.build().toByteArray());
	            Hdfs.OpenFileResponse res1 = Hdfs.OpenFileResponse.parseFrom(openfile);
	            int status = res1.getStatus();
	            //System.out.println("status "+ status); // 0 mea
        	            
	            File file = new File(OUTPUT + filename);
				InputStream in = new FileInputStream(file);
				int readBytes = 0;
				byte[] by = new byte[blockSize];
				byte[] arr;
			    while ((readBytes  = in.read(by)) != -1) {
			    	arr = Arrays.copyOfRange(by, 0, readBytes);
			        byte b[] = compName.assignBlock(g.build().toByteArray());
			        Hdfs.AssignBlockResponse res = Hdfs.AssignBlockResponse.parseFrom( b );
			        Hdfs.BlockLocations loc = res.getNewBlock();
			        index = loc.getBlockNumber();
			        String ips[] = new String[4];
			        int port = 0;
			        int i = 0;
			        for (Hdfs.DataNodeLocation dat: loc.getLocationsList()){
			        	ips[i++] = dat.getIp();
			        	port = dat.getPort();
			        }
			        		        
			        String ip1 = ips[0];
			        String ip2 = ips[1];
	            
		            Hdfs.BlockLocations.Builder g1 = Hdfs.BlockLocations.newBuilder();
		            Hdfs.DataNodeLocation.Builder location = Hdfs.DataNodeLocation.newBuilder();
		            location.setIp(ip2);
		            g1.setBlockNumber(index);
		            g1.addLocations(location);
		            Hdfs.WriteBlockRequest.Builder g2 = Hdfs.WriteBlockRequest.newBuilder();
		            g2.setBlockInfo(g1);
		            g2.addData(ByteString.copyFrom(arr));
		            byte[] response = compData.writeBlock(g2.build().toByteArray());
		            Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
			    }
			    //System.out.println("Thread" + taskDetails.getTaskId() + "Done");
			    taskDetails.setIsCompleted(true);
			    taskDetails.setMapOutputFile(filename);
			    completedMapTaskQueue.add(taskDetails);
			    
	        } catch (Exception e) {
	            System.err.println("Exception:");
	            Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
	            compName.closeFile(closeReq.build().toByteArray());
	            e.printStackTrace();
	        }
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
			
		
	}

}
