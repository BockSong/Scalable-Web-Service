/* CacheIntf.java */

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CacheIntf extends Cloud.DatabaseOps {
            public String get(String key) throws RemoteException;
            
            public boolean set(String key, String val, String auth) 
                            throws RemoteException;

            public boolean transaction(String item, float price, int qty)
                            throws RemoteException;
            
}
