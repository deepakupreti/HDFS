package com.source.HDFS;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;



import com.google.protobuf.ByteString;

public class Client {

	/**
	 * @param args
	 * @throws IOException 
	 */
	static int blockSize = 8*1024; 
	static String nameNodeIp = "192.168.43.231";
	static String OUTPUT = "/home/ankur/OUTPUT/";
	
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while(true){
			String s = br.readLine();
			String array[] = s.split("\\s+"); 
			if(array[0].equalsIgnoreCase("put")){
				Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
				g.setFileName(array[1]);
				
				INameNode compName = null ;
				try {
		            String nameNode = "nameNode";
		            Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
		            compName = (INameNode) registryNameNode.lookup(nameNode);
		            byte[] openfile = compName.openFile(g.build().toByteArray());
		            Hdfs.OpenFileResponse res1 = Hdfs.OpenFileResponse.parseFrom(openfile);
		            int status = res1.getStatus();
		            System.out.println("status "+ status); // 0 mea
		            if(status==1){
		            	System.out.println("File Already Present");
		            	continue;
		            }
		           
		            
		            File file = new File(array[1]);
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
				        String dataNode = "dataNode";
			            Registry registryDataNode = LocateRegistry.getRegistry(ip1, port);
			            IDataNode compData = (IDataNode) registryDataNode.lookup(dataNode);
			            
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
				    
				    System.out.println("Done");
		        } catch (Exception e) {
		            System.err.println("Exception:");
		            Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
		            e.printStackTrace();
		        }
			
			}
			else if(array[0].equalsIgnoreCase("get")){
				String filename = array[1];
				String nameNode = "nameNode";
				INameNode compName = null;
	            try {
	            	Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
					compName = (INameNode) registryNameNode.lookup(nameNode);
					Hdfs.OpenFileRequest.Builder g = Hdfs.OpenFileRequest.newBuilder();
					g.setFileName(array[1]);
					byte[] b = compName.openFile(g.build().toByteArray());
					
					Hdfs.OpenFileResponse res = Hdfs.OpenFileResponse.parseFrom(b);
					int status = res.getStatus();
					if(status == 0){
						System.out.println("No such file");
						continue;
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
					FileOutputStream out = new FileOutputStream(new File(OUTPUT + filename));
					for(Hdfs.BlockLocations blockloc: response.getBlockLocationsList()){
						index = blockloc.getBlockNumber();
						for(Hdfs.DataNodeLocation dataloc : blockloc.getLocationsList()){
							ip = dataloc.getIp();
							port = dataloc.getPort();
						}
						String dataNode = "dataNode";
			            Registry registryDataNode = LocateRegistry.getRegistry(ip, port);
			            IDataNode compData = (IDataNode) registryDataNode.lookup(dataNode);
			            Hdfs.ReadBlockRequest.Builder readReq = Hdfs.ReadBlockRequest.newBuilder();
			            readReq.setBlockNumber(index);
			            byte[] chunk = compData.readBlock(readReq.build().toByteArray());
						Hdfs.ReadBlockResponse readresponse = Hdfs.ReadBlockResponse.parseFrom(chunk);
						for(ByteString val : readresponse.getDataList()){
							out.write(val.toByteArray());
						}
					}
					out.close();
					Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
		            System.out.println("Done");
	            } catch (NotBoundException e) {
	            	Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
					e.printStackTrace();
				}
				
			}
			else if(array[0].equalsIgnoreCase("list")){
				String nameNode = "nameNode";
	            Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
	            INameNode compName = null;
	            
				try {
					compName = (INameNode) registryNameNode.lookup(nameNode);
					Hdfs.ListFilesRequest.Builder listReq = Hdfs.ListFilesRequest.newBuilder();
		            byte[] response = compName.list(listReq.build().toByteArray());
		            Hdfs.ListFilesResponse listresponse = Hdfs.ListFilesResponse.parseFrom(response);
		            for(String list : listresponse.getFileNamesList()){
		            	System.out.println(list);
		            }
				} catch (NotBoundException e) {
						Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
			            compName.closeFile(closeReq.build().toByteArray());
						e.printStackTrace();
				}
	          
	            
	            
				
				
			}
			else if(array[0].equalsIgnoreCase("close")){
				String nameNode = "nameNode";
	            Registry registryNameNode = LocateRegistry.getRegistry(nameNodeIp);
	            INameNode compName = null;
				try {
					compName = (INameNode) registryNameNode.lookup(nameNode);
					Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
				} catch (NotBoundException e) {
					Hdfs.CloseFileRequest.Builder closeReq = Hdfs.CloseFileRequest.newBuilder();
		            compName.closeFile(closeReq.build().toByteArray());
					e.printStackTrace();
				}
				break;
				
			}
		}

	}

}
