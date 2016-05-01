package com.source.MAPREDUCE;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote {
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] job) throws RemoteException;;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] status) throws RemoteException;;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] beat) throws RemoteException;;
}
