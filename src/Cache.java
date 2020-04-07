/* 
 * Cache.java
 */

import java.io.*;
import java.util.concurrent.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// Cache object definition
public class Cache extends UnicastRemoteObject implements CacheIntf {
	// caching for key and set
	private static ConcurrentHashMap<String, String> key_set = new 
							ConcurrentHashMap<String, String>();

	public Cache() throws RemoteException {
		super(0);
	}

	public synchronized String get(String key) throws RemoteException {
		return key_set.getOrDefault(key, null);
	}

    public synchronized boolean set(String key, String val, String auth) 
                                                throws RemoteException {
        try {
            key_set.put(key, val);
            return true;
        } catch (Exception e) {
            return false;
        }
	}

	public boolean transaction(String item, float price, int qty)
			                                    throws RemoteException {
        throw NotImplementedException;
		return false;
	}

}
