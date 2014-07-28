package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * Created by Dipak Malla
 * Date: 7/28/14
 */
public class ApiConnectionPool {
    private static HConnection hConnection;
    public static HConnection gethConnection() throws IOException, ServiceException {
        if(hConnection == null) {
            Configuration configuration = HBaseConfiguration.create();
            HBaseAdmin.checkHBaseAvailable(configuration);
            hConnection = HConnectionManager.createConnection(configuration);
        }
        return hConnection;
    }
    public static void closeHConncetion() throws IOException {
        if(hConnection != null){
            hConnection.close();
            hConnection = null;
        }
    }
}
