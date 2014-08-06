package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by Dipak Malla
 * Date: 7/28/14
 */
public class TestHbaseApi {

    @Test
    public void Insert() throws IOException, ServiceException, HbaseApiException.EmptyRecord, HbaseApiException.EmptyTableName, HbaseApiException.ValidationError {
        HbaseData hbaseData = new HbaseData();
        HbaseApi hbaseApi = new HbaseApi("kauwa");
        try {
            //hbaseData.setTable("kauwa");
            HashMap<String, HashMap<String, String>> data =new HashMap<String, HashMap<String, String>>(1);
            HashMap<String, String> val = new HashMap<String, String>(1);
            val.put("a","APPLE");
            data.put("k", val);
            hbaseData.setRow("a1");
            hbaseData.setRecord(data);
            hbaseApi.Insert(hbaseData);

        }
        finally {
                hbaseApi.Close();
        }
    }

    @Test
    public void MultipleRowMultipleColumnFetch() throws IOException, ServiceException {
        HbaseApi hbaseApi = new HbaseApi("session");
        HbaseData hbaseData = new HbaseData();
        try{
            ArrayList<String> col = new ArrayList<String>(1);
            col.add("session");
            hbaseData.setStartRow("row");
            hbaseData.setStopRow("rowx");
            hbaseData.setColFamily("s");
            hbaseData.setColumnList(col);
            HashMap<String, HashMap<String, String>> resultSet = hbaseApi.MultiRowMultiColFetch(hbaseData);

        } catch (HbaseApiException.ValidationError validationError) {
            validationError.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
