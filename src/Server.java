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
	// for inital set up
	private static double FRONT_INIT = 1.5;
	private static double MID_INIT = 3.0;
	// for scale up
	private static double FRONT_QLEN_FAC = 11.8;
	private static double MID_QLEN_FAC = 5.5;
	// for scale down
	private static int FRONT_IDLE_MAX = 1150;
	private static int MID_IDLE_MAX = 2150;
	private static int FRONT_IDLE_CONS = 550;
	private static int MID_IDLE_CONS = 650;
	private static int CONS_QUE_SIZE = 3;
	// for drop
	private static long PURCHASE_TH = 1850;
	private static long BROWSE_TH = 850;
	// for dynamic estimation
	private static long INIT_PERIOD = 4500;
	private static long ESTI_TIME = 1000;
	private static long ESTI_GAP = 35000;

	private static double LF_NOR = 1.09;
	private static double LF_LOW = 1.08;
	private static double LF_MED = 0.7;
	private static double LF_HIGH = 0.49;

	private static int L_LOW = 3;
	private static int L_MED = 8;
	private static int N_MANY_INIT = 5;
	private static int N_MANY_MID = 8;
	// other
	private static int MIN_NUM = 1;
	private static int MIN_INIT = 1;
	private static int VMID_PRIME = 1;
	private static int PORT_OFFSET = 1;
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
	// inital luanch time. used by the prime server
	private static long initTime = System.currentTimeMillis();
	// dynamic load. used by the prime server
	private static int load = 0;
	// dynamic queue load factor. used by the prime server
	private static double loadFactor = 1;
	// map the ID of VMs and their roles ("front" or "middle")
	private static ConcurrentHashMap<Integer, String> child_role = new 
							ConcurrentHashMap<Integer, String>();
	// Thread-safe Queue for requests
	private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> req_queue = new 
			   				ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
	// map the requests and the time they are added (used only by prime server)
	private static ConcurrentHashMap<Cloud.FrontEndOps.Request, Long> req_time = new 
							ConcurrentHashMap<Cloud.FrontEndOps.Request, Long>();
	// save the 3 latest interval time before requests arrive (used by mid tier servers)
	private static ConcurrentLinkedQueue<Long> req_freq = new 
							ConcurrentLinkedQueue<Long>();
	private static Object queue_lock = new Object();  // lock for accessing req_time

	private static Boolean DEBUG = true; 

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
		return vm_id == VMID_PRIME;
	}

	/*
	 * launch_frontTier: launch a new front tier server.
	 */
	private static void launch_frontTier() {
		int chl_vmID = SL.startVM();
		child_role.put(chl_vmID, "front");
		num_frontTier++;
		if (DEBUG)  System.out.println("Scaled up front tier. #: " + num_frontTier);
	}

	/*
	 * launch_midTier: launch a new middle tier server.
	 */
	private static void launch_midTier() {
		int chl_vmID = SL.startVM();
		child_role.put(chl_vmID, "middle");
		num_midTier++;
		if (DEBUG)  System.out.println("Scaled up mid tier. #: " + num_midTier);
	}

	/*
	 * shutdown: shutdown a VM.
	 */
	private static void shutdown(int vm_id) {
		SL.endVM(vm_id);
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
		if (req_freq.size() <= MIN_NUM)
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
	 * requestEnd: (a child middle tier server) request a permission to 
	 * 			   terminate itself. primary server reject it if there 
	 * 			   will be too few servers.
	 * Return: True for permiting or false for not
	 */
	public Boolean requestEnd() throws RemoteException {
		if (num_midTier >= MIN_NUM + 1) {
			num_midTier -= 1;
			return true;
		}
		return false;
	}

	/*
	 * notifyEnd: (a child front tier server) notify the prime server
	 * 			  that it will terminate itself.
	 */
	public void notifyEnd() throws RemoteException {
		num_frontTier -= 1;
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
				long cur_time = System.currentTimeMillis() - initTime;

				synchronized (queue_lock) {
					if (!req_queue.offer(r)) {
						System.out.println("UncheckedException: req_queue is full");
					}
					req_time.put(r, System.currentTimeMillis());
				}

				long i = cur_time / ESTI_GAP;
				cur_time = cur_time % ESTI_GAP;

				if (cur_time < ESTI_TIME) {
					// first interval in the very beginning
					if (i == 0) {
						if (DEBUG)  System.out.println("At interval " + i + ", q_len: " + 
														req_queue.size() + " load come ");
						// if already have many servers, slow down the speed of scaling a bit
						if (num_midTier >= N_MANY_INIT) {
							loadFactor = LF_NOR;
							if (DEBUG)  System.out.println("loadFactor: " + loadFactor);
						}
					}
					// estimate load during runtime
					else {
						load += 1;
						if (DEBUG)  System.out.print("At interval " + i + ", q_len: " + 
														req_queue.size() + " load: " + load);
						// dynamiaclly adjust load factor
						if (loadFactor == LF_NOR) {
							// if it's in normal pace, remain the same
							if (DEBUG)  System.out.println(", loadFactor: " + loadFactor);
						}
						else if (load < L_LOW) {
							// low load, slow down a bit
							loadFactor = LF_LOW;
							if (DEBUG)  System.out.println(", loadFactor: " + loadFactor);
						}
						else if (load < L_MED) {
							// medium load, speed up
							loadFactor = LF_MED;
							if (DEBUG)  System.out.println(", loadFactor: " + loadFactor);
						}
						else {
							// high load, speed up a lot
							loadFactor = LF_HIGH;
							if (DEBUG)  System.out.println(", loadFactor: " + loadFactor);
							// if don't have many servers, launch some immediately
							while (num_midTier < N_MANY_MID) {
								launch_midTier();
							}
						}
					}
				}

				// scale out front tier
				if (req_queue.size() > num_frontTier * (FRONT_QLEN_FAC * loadFactor)) {
					launch_frontTier();
				}

				// scale out middle tier
				if (req_queue.size() > num_midTier * (MID_QLEN_FAC * loadFactor)) {
					launch_midTier();
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
					System.out.println("UncheckedException: req_queue is full");
				}

				Long this_idle = endTime - startTime;

				// see if we need to scale down front tier
				if (this_idle > FRONT_IDLE_MAX || check_freq(FRONT_IDLE_CONS)) {
					prim_server.notifyEnd();
					if (DEBUG) {
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
				else {
					SL.drop(r);
				}
				startTime = System.currentTimeMillis();
			}

			endTime = System.currentTimeMillis();
			Long this_idle = endTime - startTime;

			// check if need to scale down
			if (this_idle > MID_IDLE_MAX || check_freq(MID_IDLE_CONS)) {
				// scale down only when permitted by the primary server
				if (prim_server.requestEnd()) {
					if (DEBUG) {
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
					}
					shutdown(vm_id);
				}
			}
		}
	}

	public static synchronized void main ( String args[] ) throws Exception {
		if (args.length != 3) 
			throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

		String cloud_ip = args[0];
		int cloud_port = Integer.parseInt(args[1]);
		int vm_id = Integer.parseInt(args[2]);

		SL = new ServerLib( cloud_ip, cloud_port );

		// prime server, perform as front tier and also manage child servers
		if (is_primServer(vm_id)) {
			Server server = new Server(); 
			// No need to createRegistry again
			Naming.rebind("//localhost:" + cloud_port + "/ServerIntf", server);
			if (DEBUG)  System.out.println("VM " + vm_id + " (prime server) set up.");

			// register with load balancer so requests are sent to this server
			SL.register_frontend();
	
			// Run Cache
			Cache cache = new Cache(SL);
			LocateRegistry.createRegistry(cloud_port + PORT_OFFSET);
			Naming.rebind("//localhost:" + (cloud_port + PORT_OFFSET) + "/CacheIntf", cache);
			if (DEBUG)  System.out.println("Cache DB set up.");

			// start to deal with scaling
			int tod = (int) SL.getTime();
			double CAR = get_avgCAR(tod);
			if (DEBUG)
				System.out.println("Given arrival rate: " + CAR);
	
			// Statically decide the inital number of child servers
			num_frontTier = Math.max((int) (Math.ceil(CAR * FRONT_INIT)), MIN_INIT);
			num_midTier = Math.max((int) (Math.ceil(CAR * MID_INIT)), MIN_INIT);
			if (DEBUG) {
				System.out.println("Inital front tier: " + num_frontTier);
				System.out.println("Inital middle tier: " + num_midTier);
			}
			
			int i, chl_vmID;
			// launch inital front tier servers (prime server itself has been launched)
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
												+ (cloud_port + PORT_OFFSET) + "/CacheIntf");
			} catch (Exception e) {
				System.out.println("NotBoundException in connection.");
			}

			// front tier
			if (prim_server.askRole(vm_id).equals("front")) {
				if (DEBUG)  System.out.println("VM " + vm_id + " (front tier) set up.");
				SL.register_frontend();
				frontTier(vm_id);
			}
			// middle tier
			else {
				if (DEBUG)  System.out.println("VM " + vm_id + " (mid tier) set up.");
				middleTier(vm_id);
			}
		}
	}
}
