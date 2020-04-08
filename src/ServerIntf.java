/* ServerInft.java */

import java.rmi.Remote;
import java.rmi.RemoteException;

// Indicates remote interface description
public interface ServerIntf extends Remote {
            public Boolean addRequest(Cloud.FrontEndOps.Request r, long time)
                            throws RemoteException;

            public ReqInfo getRequest()
                            throws RemoteException;

            public Boolean requestEnd()
                            throws RemoteException;
            
            public String askRole(int vm_id)
                            throws RemoteException;
            
}
