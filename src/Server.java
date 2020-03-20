/* 
 * Server.java - A scalable web service
 * 
 * A server class which implements various techniques to scale-out a simulated,
 * cloud-hosted, multi-tier web service.
 */

import java.io.*;
import java.math.*;

public class Server {
	/*
	 * is_primServer: check if a VM process is the primary server
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
	 */
	private static Boolean is_primServer (int vm_id) {
		return vm_id == 1;
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

		// serverLib is used to access the database
		ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]) );

		int vm_id = Integer.parseInt(args[2]);
		System.out.println("VM " + vm_id + " set up.");
		
		// register with load balancer so requests are sent to this server
		SL.register_frontend();

		if (is_primServer(vm_id)) {
			int tod = (int) SL.getTime();
			double CAR = get_avgCAR(tod);
			System.out.println("Given arrival rate: " + CAR);
	
			int num_chdServers = (int) (Math.ceil(CAR * 3.9)); // statically decide the num of servers
			System.out.println("num_chdServers: " + num_chdServers);
			
			int i, chl_vmID;
			// launch num_chdServers VMs to process the jobs
			for (i = 0; i < num_chdServers; i++) {
				chl_vmID = SL.startVM();
			}
		}
	
		// main loop to process jobs
		while (true) {
			Cloud.FrontEndOps.Request r = SL.getNextRequest();
			SL.processRequest( r );
		}
	}
}

