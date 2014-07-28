package com.tektak.iloop.hbaseapi;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Dipak Malla
 * Date: 7/25/14
 */
public class HbaseApi {
    private HTableInterface hTable;
    private int rowsCount = 0;
    public HbaseApi(String tableName) throws IOException, ServiceException {
        this.hTable = ApiConnectionPool.gethConnection().getTable(tableName);
    }

    public HashMap<String, String> SingleRowMultiColFetch(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if(hbaseData.getRow()== null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if(hbaseData.getColFamily()== null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if(hbaseData.getColumnList()== null || hbaseData.getColumnList().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if(this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Get get = new Get(Bytes.toBytes(hbaseData.getRow()));
        for(String cols : hbaseData.getColumnList()){
            get.addColumn(Bytes.toBytes(hbaseData.getColFamily()),Bytes.toBytes(cols));
        }
        Result result = this.hTable.get(get);
        this.setRowsCount(result.size());
        if(this.getRowsCount() >= 1){
            HashMap<String, String> resultSet = new HashMap<String, String>(hbaseData.getColumnList().size());
            for(String cols : hbaseData.getColumnList()){
                resultSet.put(cols ,Bytes.toString(result.getValue(Bytes.toBytes(hbaseData.getColFamily()),
                        Bytes.toBytes(cols))));
            }
            return  resultSet;
        }
        return null;
    }
    public void SingleRowMultiColInsert(HbaseData hbaseData) throws HbaseApiException.ValidationError,
            IOException {
        if(hbaseData.getRow()== null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if(hbaseData.getColFamily()== null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if(hbaseData.getColumnValuePair()== null || hbaseData.getColumnValuePair().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if(this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        for(Map.Entry<String, String> colValPair: hbaseData.getColumnValuePair().entrySet()) {
            put.add(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(colValPair.getKey()),
                    Bytes.toBytes(colValPair.getValue()));
        }
        this.hTable.put(put);
    }
    public void SingleRowColInsert(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if(hbaseData.getRow()== null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if(hbaseData.getColFamily()== null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if(hbaseData.getColumn()== null || hbaseData.getColumn().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if(this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        put.add(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(hbaseData.getColumn()),
                Bytes.toBytes(hbaseData.getValue()));
        this.hTable.put(put);
    }
    public String SingleRowColFetch(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if(hbaseData.getRow()== null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if(hbaseData.getColFamily()== null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if(hbaseData.getColumn()== null || hbaseData.getColumn().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if(this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Get get = new Get(Bytes.toBytes(hbaseData.getRow()));
        get.addColumn(Bytes.toBytes(hbaseData.getColFamily()),Bytes.toBytes(hbaseData.getColumn()));
        Result result = this.hTable.get(get);
        this.setRowsCount(result.size());
        byte[] resultValue = result.getValue(Bytes.toBytes(hbaseData.getColFamily()),
                Bytes.toBytes(hbaseData.getColumn()));
        return Bytes.toString(resultValue);
    }
    private void _insert(HbaseData hbaseData) throws IOException, HbaseApiException.ValidationError {
        if(hbaseData.getRow()== null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if(this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        if(hbaseData.getRecord() == null || hbaseData.getRecord().isEmpty())
            throw new HbaseApiException.ValidationError("Invalid column family");
        for(Map.Entry<String, HashMap<String, String>> colFamily : hbaseData.getRecord().entrySet()){
            for(Map.Entry<String, String> colVal : colFamily.getValue().entrySet()){
                put.add(Bytes.toBytes(colFamily.getKey()),
                        Bytes.toBytes(colVal.getKey()), Bytes.toBytes(colVal.getValue()));
            }
        }
        this.hTable.put(put);
    }
    public void Close() throws IOException {
        if(this.hTable != null)
            this.hTable.close();
    }
    public void Insert(HbaseData hbaseData) throws IOException,
            HbaseApiException.EmptyTableName, HbaseApiException.ValidationError {
        if(hbaseData == null)
            throw new HbaseApiException.ValidationError("Null HbaseData");
        this._insert(hbaseData);
    }

    public int getRowsCount() {
        return rowsCount;
    }

    public void setRowsCount(int rowsCount) {
        this.rowsCount = rowsCount;
    }
}
