package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by julina on 9/8/14.
 */
public class FetchWithTimestamp {
    private static HbaseApi hbaseApi = null;
    private static HbaseData hbaseData = new HbaseData();
    @BeforeClass
    public static void init() throws IOException, ServiceException {
        ArrayList<String> columnList = new ArrayList<>();
        columnList.add("h");

        hbaseApi = new HbaseApi("hello");
        hbaseData.setColFamily("h");
        hbaseData.setColumnList(columnList);
        hbaseData.setStartRow("row");
        hbaseData.setStopRow("rowx");
        hbaseData.setMaxTimestamp(1410160872595l);
        hbaseData.setMinTimestamp(1410160855661l);
      //  hbaseData.setTimestamp(1410160879165l);


    }
    @Test
    public void get() throws HbaseApiException.ValidationError, IOException {
        HashMap<String, HashMap<String, String>> result = hbaseApi.MultiRowMultiColFetch(hbaseData);
        for(Map.Entry<String, HashMap<String, String >> row : result.entrySet()){
            for(Map.Entry<String, String> col: row.getValue().entrySet()){
                System.out.println(col.getValue());
            }
        }
    }

    @AfterClass
    public static void close() throws IOException {
        hbaseApi.Close();
    }
}
