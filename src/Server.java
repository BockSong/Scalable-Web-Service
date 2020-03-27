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
	// ------------------ Adjustable paramaters ------------------------
	// for scale-out
	private static double QLEN_FACTOR = 1.2;
	// for scale-in
	private static int MAX_IDLE_TIME = 2000;
	private static double MIN_REQ_RATE = 0.6; // not yet used
	// for drop
	private static long PURCHASE_TH = 1900;
	private static long BROWSE_TH = 900;
	// -----------------------------------------------------------------

	private static int num_chdServers; 
	private static ServerLib SL;
	// Server Interface for the primary server
	private static ServerIntf prim_server;

	private static long startTime = System.currentTimeMillis();
	private static long endTime = System.currentTimeMillis();
	private static double response_time;
	private static double server_load;
	private static double request_rate;
	// Thread-safe Queue for requests
	private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> req_queue = new 
			   				ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
	// map the requests and the time they are added
	private static ConcurrentHashMap<Cloud.FrontEndOps.Request, Long> req_time = new 
							ConcurrentHashMap<Cloud.FrontEndOps.Request, Long>();
	private static Object queue_lock = new Object();  // lock for accessing request's time

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
		return vm_id == 1;
	}

	/*
	 * getRequest: get a request from the queue.
	 * Return: ReqInfo of the request, or null if the queue is empty
	 */
    public synchronized ReqInfo getRequest() throws RemoteException {
		Cloud.FrontEndOps.Request r = req_queue.poll();
		ReqInfo request;
		if (r != null) {
			synchronized (queue_lock) {
				request = new ReqInfo(r, req_time.get(r));
				req_time.remove(r);
			}
			return request;
		}
		return null;
	};

	/*
	 * rmChdServer: inform the primary server that a child server is going to terminate
	 * Return: True for success and false for an error
	 */
	public Boolean rmChdServer() throws RemoteException {
		try {
			num_chdServers -= 1;
			return true;
		} catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
			return false;
		}
	}

	private static void shutdown(int vm_id) {
		SL.endVM(vm_id);
		// TODO:
		//UnicastRemoteObject.unexportObject(this, true);
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

		String cloud_ip = args[0];
		int cloud_port = Integer.parseInt(args[1]);
		int vm_id = Integer.parseInt(args[2]);

		// serverLib is used to access the database
		SL = new ServerLib( cloud_ip, cloud_port );

		// front-end, only get request from clients
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
	
			// Statically decide the num of servers as a starter
			num_chdServers = (int) (Math.ceil(CAR * 3.9));
			System.out.println("num_chdServers: " + num_chdServers);
			
			// TODO: scale-out for front-end and maintain record for front-end/middle tier

			int i, chl_vmID;
			// launch num_chdServers VMs to process the jobs
			for (i = 0; i < num_chdServers; i++) {
				chl_vmID = SL.startVM();
			}

			while (true) {
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
				synchronized (queue_lock) {
					if (!req_queue.offer(r)) {
						System.out.println("uncheckedException: Request queue is full accidentally");
					}
					req_time.put(r, System.currentTimeMillis());
				}

				// scale out for app tier
				// TODO: add more conditions like long reponse time, high load
				//response_time = 0;
				//server_load = 0;
				if (req_queue.size() > num_chdServers * QLEN_FACTOR) {
					System.out.println("Scale out! Queue size: " + req_queue.size() + 
									 ", num_servers: " + num_chdServers);
					chl_vmID = SL.startVM();
					num_chdServers++;
				}
			}
		}
		// application tier, only process request
		else {
			// connect to server with Java RMI	
			try {
				prim_server = (ServerIntf) Naming.lookup("//" + cloud_ip + ":" 
												  + cloud_port + "/ServerIntf");
				System.out.println("VM " + vm_id + " (app tier) set up, connection built.");
			} catch (Exception e) {
				System.out.println("NotBoundException in connection.");
			}

			// main loop to process jobs
			while (true) {
				ReqInfo request = prim_server.getRequest();
				if (request != null) {
					Cloud.FrontEndOps.Request r = request.r;
					long upper_bound;
					if (r.isPurchase) {
						upper_bound = PURCHASE_TH;
					}
					else {
						upper_bound = BROWSE_TH;
					}
					// Drop: don't handle the request if it‘s too late
					endTime = System.currentTimeMillis();
					if (endTime - request.waiting_time < upper_bound) {
						SL.processRequest(r);
						startTime = System.currentTimeMillis();
					}
				}

				// TODO: define request_rate, add more policies
				request_rate = 1;
				endTime = System.currentTimeMillis();
				if (endTime - startTime > MAX_IDLE_TIME || request_rate < MIN_REQ_RATE) {
					System.out.println("Scale in! Waiting time: " + (endTime - startTime));
					prim_server.rmChdServer();
					shutdown(vm_id);
				}
			}
		}
	}
}
