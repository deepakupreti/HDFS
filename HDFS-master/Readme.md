How to setup

Phase1  // it will include getting, putting and listing the files present in Hdfs 
1) Run the NameNode.java as a server.
2) Now run the DataNode.java in one or multiple machines.
	->DataNode.java will read DataConfig file to get the given information.
		DataNode_Id Self_Ip DataNodePort path of outputfolder(where the Hdfs files are stored) NameNode_Ip	
		eg: 1 192.168.43.231 15000 /home/ankur/DATA 192.168.43.231

3) Run Client.java
   -> client can give any of the four commands
     a) put filename  // to put the file in hdfs 
     b) get filename  // to get the file present in hdfs
     c) list          // to list the files present in hdfs
     d) close         // will sign off the client and save the informations in files (to make system persistent).
 Important Points:
 -> File LastIndex helps us in tracking the last alloted block numbers in hdfd. Upon client log off the last writen block name is written          to 	    the file. Initialy it will contain '0' without quotes. 
-> File BlockMap will contain the names of files currently in hdfs along with their block numbers.

--------------------------

Phase2 // It will will perform mapper and recuder operations on the files present in Hdfs. If the filename given by user is not present in hdfs than the mapper reducer will not work.

1) Run JobTracker.java a server.
	-> it will read file JtConfig which will contain a line containing some information in given format.
		Self_ip:Self_Port NameNode_Ip:NameNode_Port
		eg:192.168.43.231:16000 192.168.43.231:1099 
	->It will also red file LastIds which contains three integers in given format:
		mapTaskId reduceTaskId	lastJobID
		eg:42 3 1

2) Run TaskTracker.java in one or multiple machines.
		-> by default the number of mapperthreads and reducerthreads are set to 10. You can modify them in lines 92,96 as per your needs.
		-> TaskTracker.java will initially read TTconfig which contains a line containing data in format given below:
				> TaskTracker_id self_ip:self_port jobtracker_ip:jobtracker_port path_of_toutput NameNode_Ip Blocksize
				eg: 1 192.168.43.231:17000 192.168.43.231:16000 /home/ankur/TToutput/ 192.168.43.231 8388608
3) Run JobClient
		->change the following three parameters in JobClient.java before running it
			a)	String inputfile = "input.txt";
			b)	String outputfile = "keen";
			c)	String keyword = "head";
			you can also make minor changes in code and can take the above parameters as commandline arguments
			
		->JobClient.java will read JCconfig file to get some information given below: 	
			JobTracker_ip Mapper_Name Reducer_Name
			eg:192.168.43.231:16000 Mapper Reducer
			 	


				

