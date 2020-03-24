/* ServerInft.java */

import java.rmi.Remote;
import java.rmi.RemoteException;

// Indicates remote interface description
public interface ServerIntf extends Remote {
    // TODO: change Cloud.FrontEndOps.Request to a serilization class?
    public Cloud.FrontEndOps.Request getRequest()
                  throws RemoteException;
/*
    public int getVersionID( String path )
                  throws RemoteException;
                  */
}
