/* ReqInfo.java */

import java.io.*;

// indicates object that can be copied remotely
public class ReqInfo implements Serializable {
    public Cloud.FrontEndOps.Request r;

    public ReqInfo() {}

    public ReqInfo( Cloud.FrontEndOps.Request r ) {
        this.r = r;
    }
}
