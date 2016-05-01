package com.source.HDFS;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;


public class NameNode implements INameNode{
	
	static ArrayList<String> ipList = new ArrayList<>();
	static ConcurrentHashMap<String, ArrayList<Integer> > fileToblocksMap = new ConcurrentHashMap<>();
	static HashMap<Integer, ArrayList<String>> blockToIPMap = new HashMap<>();
	static String selfIp = "192.168.43.231";
	static int nameNodePort = 1099;
	static int dataNodePort = 15000;
	static int lastIndex = 0;
	static HashMap<Integer, String> dataNodeIdToIpMap = new HashMap<>();
	public NameNode() {
		super();
	}
	  
	@Override
	public synchronized byte[] openFile(byte[] inp) throws RemoteException {
		Hdfs.OpenFileRequest req;
		Hdfs.OpenFileResponse.Builder res;
		try {
			req = Hdfs.OpenFileRequest.parseFrom(inp);
			res = Hdfs.OpenFileResponse.newBuilder();
			String fileName = req.getFileName();
			
			if(fileToblocksMap.containsKey(fileName.toLowerCase().trim())){
				res.setStatus(1);
			}
			else{
				
				res.setStatus(0);
				return res.build().toByteArray();
			}
			ArrayList<Integer> blockNumbers = fileToblocksMap.get(fileName.toLowerCase().trim());
			//Hdfs.OpenFileResponse.Builder res = Hdfs.OpenFileResponse.newBuilder();
			
			//res.setStatus(0);
			for(int i:blockNumbers){
				res.addBlockNums(i);
			}
			//res.setStatus(1);
			return res.build().toByteArray();
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public synchronized byte[] closeFile(byte[] inp) throws RemoteException {
		Hdfs.CloseFileResponse.Builder response = Hdfs.CloseFileResponse.newBuilder();
		response.setStatus(1);
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("BlockMap")); 
			for(String key : fileToblocksMap.keySet()){
				out.write(key + ":");
				for(int i : fileToblocksMap.get(key)){
					out.write(i + ",");
				}
				out.write("\n");
			}
			out.close();
			BufferedWriter out1 = new BufferedWriter(new FileWriter("LastIndex"));
			out1.write(Integer.toString(lastIndex));
			out1.close();
		} catch ( IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		try {
			Hdfs.BlockLocationRequest locReq = Hdfs.BlockLocationRequest.parseFrom(inp);
			Hdfs.BlockLocationResponse.Builder locRes = Hdfs.BlockLocationResponse.newBuilder();
			
			for(int i : locReq.getBlockNumsList()){
				Hdfs.BlockLocations.Builder res = Hdfs.BlockLocations.newBuilder();
				Hdfs.DataNodeLocation.Builder dataLoc = Hdfs.DataNodeLocation.newBuilder();
				dataLoc.setIp(blockToIPMap.get(i).get(0));
				dataLoc.setPort(dataNodePort);
				res.setBlockNumber(i);
				res.addLocations(dataLoc);
				locRes.addBlockLocations(res);
			}
			return locRes.build().toByteArray();
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public synchronized byte[] assignBlock(byte[] inp) throws RemoteException {
		String s = "";
		try {
			Hdfs.OpenFileRequest in = Hdfs.OpenFileRequest.parseFrom( inp );
			s = in.getFileName();
//			if(fileToblocksMap.containsKey(s.toLowerCase().trim())){
//				Hdfs.AssignBlockResponse.Builder res = Hdfs.AssignBlockResponse.newBuilder();
//				res.setStatus(1);
//				return res.build().toByteArray();
//			}
			int size = ipList.size();
			//System.out.println(size);
			Random r = new Random();
			int idx1 = r.nextInt(size);
			int idx2;
			while(true){
				idx2 = r.nextInt(size);
				if(idx2 == idx1){
					continue;
				}
				else
					break;
			}
			String ip1 = ipList.get(idx1);
			String ip2 = ipList.get(idx2);
									
			Hdfs.BlockLocations.Builder loc = Hdfs.BlockLocations.newBuilder();
			
			Hdfs.DataNodeLocation.Builder dat = Hdfs.DataNodeLocation.newBuilder();
			dat.setIp(ip1);
			dat.setPort(dataNodePort);
			loc.addLocations(dat);
			
			Hdfs.DataNodeLocation.Builder dat1 = Hdfs.DataNodeLocation.newBuilder();
			dat1.setIp(ip2);
			dat1.setPort(dataNodePort);
			loc.addLocations(dat1);
			
			
			String key = s.toLowerCase().trim();
			if(fileToblocksMap.containsKey(key)){
				fileToblocksMap.get(key).add(lastIndex);	
			}
			else{
				ArrayList<Integer> blocksNum =new ArrayList<>();
				blocksNum.add(lastIndex);
				fileToblocksMap.put(key.toLowerCase().trim(), blocksNum);
			}
			loc.setBlockNumber(lastIndex);
			ArrayList<String> temp = new ArrayList<>();
			temp.add(ip1);
			blockToIPMap.put(lastIndex,temp);
			lastIndex++;
			
			Hdfs.AssignBlockResponse.Builder out = Hdfs.AssignBlockResponse.newBuilder();
			out.setNewBlock(loc);
			return out.build().toByteArray();
		
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return inp;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		Hdfs.ListFilesResponse.Builder response = Hdfs.ListFilesResponse.newBuilder();
		for(String key : fileToblocksMap.keySet()){
			response.addFileNames(key);
		}
		response.setStatus(1);
		return response.build().toByteArray();
		
	}

	@Override
	public synchronized byte[] blockReport(byte[] inp) throws RemoteException {
		try {
			Hdfs.BlockReportRequest req = Hdfs.BlockReportRequest.parseFrom(inp);
			dataNodeIdToIpMap.put(req.getId(), req.getLocation().getIp());
			
			for(int i: req.getBlockNumbersList()){
				//System.out.println(blockToIPMap.get(i));
				//System.out.println(i);
				if(blockToIPMap.containsKey(i) && blockToIPMap.get(i).size() == 1){
					if(blockToIPMap.get(i).get(0).equals(req.getLocation().getIp())){
						
					}
					else{
						blockToIPMap.get(i).add(req.getLocation().getIp());
					}
				}
				else{
					ArrayList<String> temp = new ArrayList<>();
					temp.add(req.getLocation().getIp());
					blockToIPMap.put(i, temp);
				}
					
					
			}

			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		Hdfs.HeartBeatResponse.Builder res = Hdfs.HeartBeatResponse.newBuilder();
		res.setStatus(1);
		return res.build().toByteArray();
	}

	public static void main(String args[]) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("Nameconfig"));
		String s = null;
		while((s=br.readLine()) != null){
			String str[] = s.split("\\s+");
			dataNodeIdToIpMap.put(Integer.parseInt(str[0].trim()), str[1]);
			ipList.add(str[1]);
		}
		BufferedReader br1 = new BufferedReader(new FileReader("BlockMap"));
		s = null;
		while((s=br1.readLine()) != null){
			String str[] = s.split(":");
			String str1[] = str[1].split(",");
			ArrayList<Integer> blockNums = new ArrayList<>();
			for(int i=0;i<str1.length - 1 ; i++){
				blockNums.add(Integer.parseInt(str1[i]));
			}
			fileToblocksMap.put(str[0].toLowerCase().trim(), blockNums);
		}
		
		BufferedReader br2 = new BufferedReader(new FileReader("LastIndex"));
		s = null;
		while((s=br2.readLine()) != null){
			lastIndex = Integer.parseInt(s.trim());
		}
		br.close();br1.close();br2.close();
		
		try {
            String name = "nameNode";
            
            java.rmi.registry.LocateRegistry.createRegistry(1099);
            INameNode engine = new NameNode();
            INameNode stub =
                (INameNode) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry(selfIp, nameNodePort);  // Give IP address of hosting server and the rmi port on which binding is to be done!
            registry.rebind(name, stub);
            System.out.println("NameNode bound");
        } catch (Exception e) {
            System.err.println("NameNode exception:");
            e.printStackTrace();
        }
		
	}


}
