package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        //hbaseData.setMaxTimestamp(1410160872595l);
        // hbaseData.setMinTimestamp(1410160855661l);
        //  hbaseData.setTimestamp(1410160879165l);


    }

    // @Test
    public void get() throws HbaseApiException.ValidationError, IOException {
        HashMap<String, HashMap<String, String>> result = hbaseApi.MultiRowMultiColFetch(hbaseData);
        for (Map.Entry<String, HashMap<String, String>> row : result.entrySet()) {
            for (Map.Entry<String, String> col : row.getValue().entrySet()) {
                System.out.println(col.getValue());
            }
        }
    }

    //@Test
    public void test() throws HbaseApiException.ValidationError, IOException {
        hbaseData.setStartRow("row");
        hbaseData.setStopRow("rowx");
        hbaseData.setColFamily("h");
        HashMap<String, HashMap<String, String>> result = hbaseApi.MultiRowMultiColFetchWithColumnTimestamp(hbaseData);
        System.out.println(result);
    }

    @Test
    public void multipleRowInsertTest() throws HbaseApiException.ValidationError, IOException {
        List<HbaseData> hbaseDatas = new ArrayList<>();
        HbaseData hbaseData = new HbaseData();
        hbaseData.setColFamily("h");
        hbaseData.setRow("row12");
        hbaseData.setColumn("h12");
        hbaseData.setValue("multiple12");
        hbaseDatas.add(hbaseData);
        HbaseData hbaseData1 = new HbaseData();
        hbaseData1.setColFamily("h");
        hbaseData1.setColumn("h13");
        hbaseData1.setRow("row13");
        hbaseData1.setValue("multiple13");
        hbaseDatas.add(hbaseData1);
        hbaseApi.MultipleRowInsertInBatch(hbaseDatas);
    }

    @AfterClass
    public static void close() throws IOException {
        hbaseApi.Close();
    }
}
