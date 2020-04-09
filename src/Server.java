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
	private static double FRONT_QLEN_FAC = 6.8; // 5/6.8
	private static double MID_QLEN_FAC = 3.5; // 3.0/5
	// for scale down
	private static int FRONT_IDLE_MAX = 1100;
	private static int MID_IDLE_MAX = 2300;
	private static int FRONT_IDLE_CONS = 700;
	private static int MID_IDLE_CONS = 700;
	private static int CONS_QUE_SIZE = 3;
	// for drop
	private static long PURCHASE_TH = 2000;
	private static long BROWSE_TH = 1000;
	// -----------------------------------------------------------------

	// number of child servers
	private static int num_frontTier; 
	private static int num_midTier; 
	// serverLib to access the database
	private static ServerLib SL;
	// interface for the primary server (used by child servers)
	private static ServerIntf prim_server;
	// interface for the Cache DB (used by middle tier servers)
	private static CacheIntf db_cache;

	private static long startTime = System.currentTimeMillis();
	private static long endTime = System.currentTimeMillis();
	// map the ID of VMs and their roles ("front" or "middle")
	private static ConcurrentHashMap<Integer, String> child_role = new 
							ConcurrentHashMap<Integer, String>();
	// Thread-safe Queue for requests
	private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> req_queue = new 
			   				ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
	// map the requests and the time they are added (used only by prime server)
	private static ConcurrentHashMap<Cloud.FrontEndOps.Request, Long> req_time = new 
							ConcurrentHashMap<Cloud.FrontEndOps.Request, Long>();
	// save the 3 latest interval time before requests arrive (used by each mid tier server)
	private static ConcurrentLinkedQueue<Long> req_freq = new 
							ConcurrentLinkedQueue<Long>();
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
	private static Boolean is_primServer(int vm_id) {
		return vm_id == 1;
	}

	/*
	 * shutdown: shutdown a VM.
	 */
	private static void shutdown(int vm_id) {
		SL.endVM(vm_id);
		// TODO: end VMs super cleanly (if needed)
		//UnicastRemoteObject.unexportObject(this, true);
	}

	/*
	 * update_freq: add the latest interval time to the queue and
	 * 				remove the earliest one (if the queue is full).
	 * Return: True for success and false for error.
	 */
	private static Boolean update_freq(long this_idle) {
		try {
			// if the queue is full, remove the head element
			if (req_freq.size() >= CONS_QUE_SIZE) {
				req_freq.poll();
			}
			req_freq.offer(this_idle);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/*
	 * check_freq: check if every 3 latest time are all above the given
	 * 			   threshold. If so, say it's idle and try to shutdown it.
	 * Return: True for idle (to be shutdown) and false for not.
	 */
	private static Boolean check_freq(int th) {
		if (req_freq.size() <= 1)
			return false;
		for (Long time: req_freq) {
			if (time <= th) {
				return false;
			}
		}
		return true;
	}

	/*
	 * addRequest: (from a child server) add a request to the queue.
	 * Return: true for success or false for failure.
	 */
	public synchronized Boolean addRequest(Cloud.FrontEndOps.Request r, long time) 
												throws RemoteException {
		synchronized (queue_lock) {
			if (!req_queue.offer(r)) {
				return false;
			}
			req_time.put(r, time);
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
			startTime = System.currentTimeMillis();
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

				// scale out front tier
				if (req_queue.size() > num_frontTier * FRONT_QLEN_FAC) {
					chl_vmID = SL.startVM();
					child_role.put(chl_vmID, "front");
					num_frontTier++;
					System.out.println("Scaled up front tier. #: " + num_frontTier);
				}

				// scale out middle tier
				if (req_queue.size() > num_midTier * MID_QLEN_FAC) {
					chl_vmID = SL.startVM();
					child_role.put(chl_vmID, "middle");
					num_midTier++;
					System.out.println("Scaled up mid tier. #: " + num_midTier);
				}
			}
			// if this is a child server
			else {
				// update the last 3 time records queue
				if (!update_freq(System.currentTimeMillis() - startTime)) {
					System.out.println("Error in updating req_freq.");
				}

				endTime = System.currentTimeMillis();
				if (!prim_server.addRequest(r, endTime)) {
					System.out.println("uncheckedException: Request queue is full accidentally");
				}

				Long this_idle = endTime - startTime;

				// see if we need to scale down front tier
				if (this_idle > FRONT_IDLE_MAX || check_freq(FRONT_IDLE_CONS)) {
					System.out.print("Scaled down front tier. ");
					if (this_idle > FRONT_IDLE_MAX) {
						System.out.println("Current idle: " + this_idle);
					}
					else {
						System.out.print("Last 3 time: ");
						for (Long time: req_freq)
							System.out.print(time + " ");
						System.out.println(" ");
					}
					SL.unregister_frontend();
					shutdown(vm_id);
				}
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
				// update the last 3 time records queue
				if (!update_freq(System.currentTimeMillis() - startTime)) {
					System.out.println("Error in updating req_freq.");
				}
	
				Cloud.FrontEndOps.Request r = request.r;
				endTime = System.currentTimeMillis();
				long upper_bound;
				
				// Drop (ignore) the request if it's already too late
				if (r.isPurchase) {
					upper_bound = PURCHASE_TH;
				}
				else {
					upper_bound = BROWSE_TH;
				}
				if (endTime - request.waiting_time < upper_bound) {
					// pass all requests to cache
					SL.processRequest(r, db_cache);
				}
				startTime = System.currentTimeMillis();
			}

			endTime = System.currentTimeMillis();
			Long this_idle = endTime - startTime;

			// check if need to scale down
			if (this_idle > MID_IDLE_MAX || check_freq(MID_IDLE_CONS)) {
				// scale down only when permitted by the primary server
				if (prim_server.requestEnd()) {
					System.out.print("Scaled down mid tier. ");
					if (this_idle > MID_IDLE_MAX) {
						System.out.println("Current idle: " + this_idle);
					}
					else {
						System.out.print("Last 3 time: ");
						for (Long time: req_freq)
							System.out.print(time + " ");
						System.out.println(" ");
					}
					shutdown(vm_id);
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
			Server server = new Server(); 
			// No need to createRegistry again
			Naming.rebind("//localhost:" + cloud_port + "/ServerIntf", server);
			System.out.println("VM " + vm_id + " (prime server) set up.");

			// register with load balancer so requests are sent to this server
			SL.register_frontend();
	
			// Run Cache
			Cache cache = new Cache(SL);
			LocateRegistry.createRegistry(cloud_port + 1);
			Naming.rebind("//localhost:" + (cloud_port + 1) + "/CacheIntf", cache);
			System.out.println("Cache DB set up.");

			// start to deal with scaling
			int tod = (int) SL.getTime();
			double CAR = get_avgCAR(tod);
			System.out.println("Given arrival rate: " + CAR);
	
			// Statically decide the inital number of child servers
			num_frontTier = Math.max((int) (Math.ceil(CAR * 1.5)), 1);
			num_midTier = Math.max((int) (Math.ceil(CAR * 3.0)), 1);
			System.out.println("Inital front tier: " + num_frontTier);
			System.out.println("Inital middle tier: " + num_midTier);
			
			int i, chl_vmID;
			// launch inital front tier servers
			for (i = 0; i < num_frontTier - 1; i++) {
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
				db_cache = (CacheIntf) Naming.lookup("//" + cloud_ip + ":" 
												+ (cloud_port + 1) + "/CacheIntf");
			} catch (Exception e) {
				System.out.println("NotBoundException in connection.");
			}

			// front tier
			if (prim_server.askRole(vm_id).equals("front")) {
				System.out.println("VM " + vm_id + " (front tier) set up, connection built.");
				SL.register_frontend();
				frontTier(vm_id);
			}
			// middle tier
			else {
				System.out.println("VM " + vm_id + " (mid tier) set up, connection built.");
				middleTier(vm_id);
			}
		}
	}
}
