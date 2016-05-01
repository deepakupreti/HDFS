package com.source.HDFS;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class DataNode extends Thread implements IDataNode {

	/**
	 * @param args
	 */
	static int dataNodePort;// = 15000;
	static String nameNodeIp;// = "192.168.43.231";
	static String selfIp;// = "192.168.43.231";
	static String storage;// = "/home/ankur/DATA";
	static int DataNodeID;// = 1;
	
	private Thread t;
	private String threadName;
	DataNode(){}
	DataNode( String name){
	       threadName = name;
	       //System.out.println("Creating " +  threadName );
	}
	
	public void run() {
		
	      try {
	    	  if(threadName.equalsIgnoreCase("HeartBeat Thread")){
		    	  	while(true){
		    	  		String nameNode = "nameNode";
			            Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
			            INameNode compName = null ;
			            compName = (INameNode) registryNameNode.lookup(nameNode);
			            Hdfs.HeartBeatRequest.Builder req = Hdfs.HeartBeatRequest.newBuilder();
			            req.setId(DataNodeID);
			            compName.heartBeat(req.build().toByteArray());
			            System.out.println("_/\\_/\\__/\\_ " + selfIp);
		    	  		Thread.sleep(20000);
		    	  	}
	    	  }else{
	    		  while(true){
		    		  String nameNode = "nameNode";
		    		  Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
		    		  INameNode compName = null ;
		    		  compName = (INameNode) registryNameNode.lookup(nameNode);
		    		  Hdfs.BlockReportRequest.Builder req = Hdfs.BlockReportRequest.newBuilder();
		    		  req.setId(DataNodeID);
		    		  Hdfs.DataNodeLocation.Builder loc = Hdfs.DataNodeLocation.newBuilder();
		    		  loc.setIp(selfIp);
		    		  loc.setPort(dataNodePort);
		    		  req.setLocation(loc);
		    		  File[] files = new File(storage).listFiles();
		    		  for (File file : files) {
		    		  if (file.isFile()) {
				   			req.addBlockNumbers(Integer.parseInt(file.getName()));
		    		   	}
		    		  }
		    		  compName.blockReport(req.build().toByteArray());
		    		  Thread.sleep(100);
	    		  }
	    		  
	    	  }
	            
	         
	     } catch (InterruptedException | RemoteException | NotBoundException e) {
	         System.out.println("Thread " +  threadName + " interrupted.");
	     }
	     
	   }
	
	public void start (){
	      //System.out.println("Starting " +  threadName );
	      if (t == null){
	         t = new Thread (this, threadName);
	         t.start ();
	      }
   }
	
	
	
	public static void main(String[] args) throws FileNotFoundException {
		BufferedReader br = new BufferedReader(new FileReader("DataConfig"));
		String s = null;
		try {
			while((s=br.readLine()) != null){
				String str[] = s.split("\\s+");
				DataNodeID = Integer.parseInt(str[0].trim());
				selfIp = str[1];
				dataNodePort = Integer.parseInt(str[2].trim());
				storage = str[3];
				nameNodeIp = str[4];
				//ipList.add(str[1]);
			}
		} catch (NumberFormatException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		DataNode T1 = new DataNode( "HeartBeat Thread");
	    //T1.start();
	    DataNode T2 = new DataNode( "BlockReport Thread");
	    T2.start();
		try {
            String name = "dataNode";
            java.rmi.registry.LocateRegistry.createRegistry(dataNodePort);
            IDataNode engine = new DataNode();
            IDataNode stub =
                (IDataNode) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry(selfIp , dataNodePort);  // Give IP address of hosting server and the rmi port on which binding is to be done!
            registry.rebind(name, stub);
            System.out.println("DataNode bound "+ selfIp);
        } catch (Exception e) {
            System.err.println("DataNode exception: "+selfIp);
            e.printStackTrace();
        }

	}

	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		try {
			Hdfs.ReadBlockRequest readreq = Hdfs.ReadBlockRequest.parseFrom(inp);
			int index = readreq.getBlockNumber();
			//System.out.println(index);
			File file = new File(storage + "/" + index);
			InputStream in = new FileInputStream(file);
			int readBytes = 0;
			Path filePath = Paths.get(storage, Integer.toString(index));
			byte[] by = Files.readAllBytes(filePath);
			
			Hdfs.ReadBlockResponse.Builder response = Hdfs.ReadBlockResponse.newBuilder();
			response.addData(ByteString.copyFrom(by));
			
			return response.build().toByteArray();
			
			
			
			
			
			
			
			
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String checker() throws RemoteException{
		return "Hello";
	}

	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		try {
			Hdfs.WriteBlockRequest in = Hdfs.WriteBlockRequest.parseFrom(inp);
			Hdfs.BlockLocations blk = in.getBlockInfo();
			
			
			int index = blk.getBlockNumber();
			//System.out.println("writing block "+index+" ");
			ByteString sbtr = null;
			for(ByteString temp: in.getDataList()){
				sbtr = temp;
			}
			FileOutputStream out = new FileOutputStream(storage + "/" + Integer.toString(index));
			out.write(sbtr.toByteArray());
			out.close();
			
			String ip2 = null;
			for(Hdfs.DataNodeLocation data: blk.getLocationsList()){
				ip2 = data.getIp();
				//System.out.println(ip2 + " "+ selfIp);
			}
			//System.out.println(ip2 + " "+ selfIp);
//			if(ip2.equals(selfIp)){
//				Hdfs.WriteBlockResponse.Builder res_write = Hdfs.WriteBlockResponse.newBuilder();
//				res_write.setStatus(1);
//				return res_write.build().toByteArray();
//			}
			String dataNode = "dataNode";
            Registry registryDataNode = LocateRegistry.getRegistry(ip2, dataNodePort);
            IDataNode compData = (IDataNode) registryDataNode.lookup(dataNode);
            
            Hdfs.BlockLocations.Builder g1 = Hdfs.BlockLocations.newBuilder();
            Hdfs.DataNodeLocation.Builder location = Hdfs.DataNodeLocation.newBuilder();
            location.setIp(ip2);
            
            g1.setBlockNumber(index);
            g1.addLocations(location);
            Hdfs.WriteBlockRequest.Builder g2 = Hdfs.WriteBlockRequest.newBuilder();
            g2.setBlockInfo(g1);
            g2.addData(sbtr);
            byte[] response1 = compData.writeBlock1(g2.build().toByteArray());
            
            Hdfs.WriteBlockResponse.Builder response = Hdfs.WriteBlockResponse.newBuilder();
			response.setStatus(1);
			
			
			return response.build().toByteArray();
			

		} catch (IOException | NotBoundException  e) {
			e.printStackTrace();
		}
		return inp;
		
		// TODO Auto-generated method stub
		
	}
	
	
	
	
	public byte[] writeBlock1(byte[] inp) throws RemoteException {
		try {
			Hdfs.WriteBlockRequest in = Hdfs.WriteBlockRequest.parseFrom(inp);
			Hdfs.BlockLocations blk = in.getBlockInfo();
			
			int index = blk.getBlockNumber();
			ByteString sbtr = null;
			for(ByteString temp: in.getDataList()){
				sbtr = temp;
			}
			FileOutputStream out = new FileOutputStream(storage + "/" + Integer.toString(index));
			out.write(sbtr.toByteArray());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return inp;
		
		// TODO Auto-generated method stub
		
	}

}
