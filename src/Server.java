/* 
 * 
 * Name: Rong Song
 * Andrew ID: rongsong
 * 
 * Server.java - A scalable web service
 * 
 * A server class which implements various techniques to scale out a simulated,
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
	// for scale up
	private static double QLEN_FACTOR = 1.6;
	// for scale down
	private static int MAX_IDLE_TIME = 2300;
	private static double MIN_REQ_RATE = 0.6; // not yet used
	// for drop
	private static long PURCHASE_TH = 2000;
	private static long BROWSE_TH = 1000;
	// -----------------------------------------------------------------

	// number of child servers
	private static int num_frontTier; 
	private static int num_midTier; 
	// serverLib to access the database
	private static ServerLib SL;
	// Server Interface for the primary server (used by child servers)
	private static ServerIntf prim_server;

	private static long startTime = System.currentTimeMillis();
	private static long endTime = System.currentTimeMillis();
	private static double response_time;
	private static double server_load;
	private static double request_rate;
	// map the ID of VMs and their roles ("front" or "middle")
	private static ConcurrentHashMap<Integer, String> child_role = new 
							ConcurrentHashMap<Integer, String>();
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
	 * Return: True for prime server and false for child server
	 */
	private static Boolean is_primServer (int vm_id) {
		return vm_id == 1;
	}

	private static void shutdown(int vm_id) {
		SL.endVM(vm_id);
		// TODO: end VMs super cleanly (if needed)
		//UnicastRemoteObject.unexportObject(this, true);
	}

	/*
	 * addRequest: (from a child server) add a request to the queue.
	 * Return: true for success or false for failure.
	 */
	public synchronized Boolean addRequest(Cloud.FrontEndOps.Request r) 
												throws RemoteException {
		if (!req_queue.offer(r)) {
			return false;
		}
		return true;
	};

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
	 * requestEnd: (child server) request a permission to terminate itself.
	 * 			  primary server reject it if there will be too few servers.
	 * Return: True for permiting or false for not
	 */
	public Boolean requestEnd() throws RemoteException {
		if (num_midTier >= 2) {
			num_midTier -= 1;
			return true;
		}
		return false;
	}

	/*
	 * askRole: (child server) ask the prime server for the role.
	 * Return: a string indicates its role. ("front" or "middle")
	 */
	public String askRole(int vm_id) throws RemoteException {
		return child_role.get(vm_id);
	}

	/*
	 * frontTier: perform as front tier server, only get request from clients.
	 */
	private static synchronized void frontTier(int vm_id) throws Exception {
		while (true) {
			Cloud.FrontEndOps.Request r = SL.getNextRequest();

			// if this is a prime server
			if (is_primServer(vm_id)) {
				int chl_vmID;
				synchronized (queue_lock) {
					if (!req_queue.offer(r)) {
						System.out.println("uncheckedException: Request queue is full accidentally");
					}
					req_time.put(r, System.currentTimeMillis());
				}

				// TODO: scale out front tier

				// scale out middle tier
				// TODO: add more conditions like long reponse time, high load
				response_time = 0;
				server_load = 0;
				if (req_queue.size() > num_midTier * QLEN_FACTOR) {
					System.out.println("Scale up! Queue size: " + req_queue.size() + 
									", num_midTier: " + num_midTier);
					chl_vmID = SL.startVM();
					child_role.put(chl_vmID, "middle");
					num_midTier++;
				}
			}
			// if this is a child server
			else {
				synchronized (queue_lock) {
					if (!prim_server.addRequest(r)) {
						System.out.println("uncheckedException: Request queue is full accidentally");
					}
					req_time.put(r, System.currentTimeMillis());
				}

				// TODO: scale down front tier
			}
		}
	}

	/*
	 * middleTier: perform as middle tier server, only process request.
	 */
	private static synchronized void middleTier(int vm_id) throws Exception {
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
				// Drop: don't handle the request if it's already too late
				endTime = System.currentTimeMillis();
				if (endTime - request.waiting_time < upper_bound) {
					SL.processRequest(r);
					startTime = System.currentTimeMillis();
				}
			}

			// check if need to scale down
			// TODO: define request_rate, add more policies
			request_rate = 1;
			endTime = System.currentTimeMillis();
			if (endTime - startTime > MAX_IDLE_TIME || request_rate < MIN_REQ_RATE) {
				// scale down only when permitted by the primary server
				if (prim_server.requestEnd()) {
					shutdown(vm_id);
					System.out.println("Scale down! Idle time: " + (endTime - startTime));
				}
			}
		}
	}

	public static synchronized void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

		String cloud_ip = args[0];
		int cloud_port = Integer.parseInt(args[1]);
		int vm_id = Integer.parseInt(args[2]);

		SL = new ServerLib( cloud_ip, cloud_port );

		// prime server, perform as front tier and also manage child servers
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
	
			// Statically decide the inital number of child servers
			num_frontTier = (int) (Math.ceil(CAR * 1.3));
			num_midTier = (int) (Math.ceil(CAR * 3.9));
			System.out.println("num_frontTier: " + num_frontTier);
			System.out.println("num_midTier: " + num_midTier);
			
			int i, chl_vmID;
			// launch inital front tier servers
			for (i = 0; i < num_frontTier; i++) {
				chl_vmID = SL.startVM();
				child_role.put(chl_vmID, "front");
			}

			// launch inital mid tier servers
			for (i = 0; i < num_midTier; i++) {
				chl_vmID = SL.startVM();
				child_role.put(chl_vmID, "middle");
			}

			frontTier(vm_id);
		}
		// child servers, front-tier or middle-tier
		else {
			// connect to server with Java RMI	
			try {
				prim_server = (ServerIntf) Naming.lookup("//" + cloud_ip + ":" 
												  + cloud_port + "/ServerIntf");
				System.out.println("VM " + vm_id + " (app tier) set up, connection built.");
			} catch (Exception e) {
				System.out.println("NotBoundException in connection.");
			}

			// front tier
			if (prim_server.askRole(vm_id) == "front") {
				frontTier(vm_id);
			}
			// middle tier
			else {
				middleTier(vm_id);
			}
		}
	}
}
