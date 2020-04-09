/* 
 * Cache.java
 */

import java.io.*;
import java.util.concurrent.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

// Cache object definition
public class Cache extends UnicastRemoteObject implements CacheIntf {
	// db object to get value cached to local
	private static Cloud.DatabaseOps db;
	// caching for key and set
	private static ConcurrentHashMap<String, String> key_val = new 
							ConcurrentHashMap<String, String>();

	public Cache(ServerLib SL) throws RemoteException {
		db = SL.getDB();
	}

	public synchronized String get(String key) throws RemoteException {
        // if miss, pull down first
        if (!key_val.containsKey(key)) {
            key_val.put(key, db.get(key));
        }
		return key_val.get(key);
	}

    public synchronized boolean set(String key, String val, String auth) 
                                                throws RemoteException {
        return db.set(key, val, auth);
	}

	public boolean transaction(String item, float price, int qty)
			                                    throws RemoteException {
        return db.transaction(item, price, qty);
	}

}
