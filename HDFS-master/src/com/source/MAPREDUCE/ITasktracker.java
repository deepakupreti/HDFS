package com.source.MAPREDUCE;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ITasktracker extends Remote  {
	void process() throws RemoteException;;

}
