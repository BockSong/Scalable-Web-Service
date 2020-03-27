/* ReqInfo.java */

import java.io.*;

// indicates object that can be copied remotely
public class ReqInfo implements Serializable {
    public Cloud.FrontEndOps.Request r;
    public long waiting_time;

    public ReqInfo() {}

    public ReqInfo( Cloud.FrontEndOps.Request r, long waiting_time ) {
        this.r = r;
        this.waiting_time = waiting_time;
    }
}
