package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Dipak Malla
 * Date: 7/28/14
 */
public class TestHbaseApi {

    @Test
    public void Insert() throws IOException, ServiceException {
        HbaseData hbaseData = new HbaseData();
        HbaseApi hbaseApi = new HbaseApi("kauwa");
        try {
            hbaseData.setTable("kauwa");
            HashMap<String, HashMap<String, String>> data =new HashMap<String, HashMap<String, String>>(1);
            HashMap<String, String> val = new HashMap<String, String>(1);
            val.put("a","APPLE");
            data.put("k", val);
            hbaseData.setRow("a1");
            hbaseData.setRecord(data);
            hbaseApi.Insert(hbaseData);
            hbaseApi.Close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HbaseApiException.EmptyTableName emptyTableName) {
            emptyTableName.printStackTrace();
        } catch (HbaseApiException.ValidationError validationError) {
            validationError.printStackTrace();
        } catch (HbaseApiException.EmptyRecord emptyRecord) {
            emptyRecord.printStackTrace();
        }
    }
}
