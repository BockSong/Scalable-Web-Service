/* 
 * Server.java - A scalable web service
 * 
 * A server class which implements various techniques to scale-out a simulated,
 * cloud-hosted, multi-tier web service.
 */

public class Server {
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
			System.out.println("Time of Day: " + tod);
	
			int num_servers = tod / 2; // TODO: statically decide the num of servers
			
			int i, chl_vmID;
			// launch num_servers VMs to process the jobs
			for (i = 0; i < num_servers; i++) {
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

