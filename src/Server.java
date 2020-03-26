/* 
 * 
 * Name: Rong Song
 * Andrew ID: rongsong
 * 
 * Server.java - A scalable web service
 * 
 * A server class which implements various techniques to scale-out a simulated,
 * cloud-hosted, multi-tier web service.
 * 
 */

import java.io.*;
import java.math.*;
import java.util.concurrent.*;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.*;

// Server object definition
public class Server extends UnicastRemoteObject implements ServerIntf {
	// Server Interface for the primary server
	private static ServerIntf prim_server;
	// Thread-safe Queue for requests
	private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> request_queue = 
			   				new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
	
	public Server() throws RemoteException {
		super(0);
	}
					 
	/*
	 * get_avgCAR: given a time of day (hour), return the average client arrival rate
	 */
	private static double get_avgCAR (int hour) {
		double[] avgCAR = {0.5, 
						  0.3, 0.1, 0.1, 0.1, 0.2, 
						  0.3, 0.7, 1.0, 0.8, 0.8,
						  0.8, 1.0, 1.1, 1.0, 0.8,
						  0.7, 0.8, 1.0, 1.2, 1.5,
						  1.4, 1.0, 0.8};
		return avgCAR[hour];
	}

	/*
	 * is_primServer: check if a VM process is the primary server
	 * Return: True for prime server and false for secondary server
	 */
	private static Boolean is_primServer (int vm_id) {
		// TODO: cannot use this if need multiple front-end servers
		return vm_id == 1;
	}

    public synchronized ReqInfo getRequest()
                  throws RemoteException {
		Cloud.FrontEndOps.Request r = request_queue.poll();
		ReqInfo request = new ReqInfo(r);
		return request;
	};

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

		String cloud_ip = args[0];
		int cloud_port = Integer.parseInt(args[1]);
		int vm_id = Integer.parseInt(args[2]);

		// serverLib is used to access the database
		ServerLib SL = new ServerLib( cloud_ip, cloud_port );

		// front-end 
		if (is_primServer(vm_id)) {
			// No need to createRegistry again
			Server server = new Server(); 
			Naming.rebind("//localhost:" + cloud_port + "/ServerIntf", server);
			System.out.println("VM " + vm_id + " (front-end) set up.");

			// register with load balancer so requests are sent to this server
			SL.register_frontend();
	
			// start to deal with scaling
			int tod = (int) SL.getTime();
			double CAR = get_avgCAR(tod);
			System.out.println("Given arrival rate: " + CAR);
	
			int num_chdServers = (int) (Math.ceil(CAR * 3.9)); // statically decide the num of servers
			System.out.println("num_chdServers: " + num_chdServers);
			
			// TODO: check if need to scale-out for front-end

			int i, chl_vmID;
			// launch num_chdServers VMs to process the jobs
			for (i = 0; i < num_chdServers; i++) {
				chl_vmID = SL.startVM();
			}
			// TODO: check if need to scale-out or shrink for app tier

			while (true) {
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
				if (!request_queue.offer(r)) {
					System.out.println("uncheckedException: Request queue is full accidentally");
				}
				//System.out.println("Request queue length: " + request_queue.size());
			}
		}
		// application tier
		else {
			// connect to server with Java RMI	
			try {
				prim_server = (ServerIntf) Naming.lookup("//" + cloud_ip + ":" + cloud_port + "/ServerIntf");
				System.out.println("VM " + vm_id + " (app tier) set up, connection built.");
			} catch (Exception e) {
				System.out.println("NotBoundException in connection.");
			}

			// main loop to process jobs
			while (true) {
				ReqInfo request = prim_server.getRequest();
				if (request.r != null) {
					SL.processRequest( request.r );
					//System.out.println("Handle request successfully.");
				}
			}
		}
	}
}

