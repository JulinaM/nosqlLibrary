package com.tektak.iloop.hbaseapi;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

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

    public HashMap<String, HashMap<String, String>> MultiRowMultiColFetch(HbaseData hbaseData) throws
            HbaseApiException.ValidationError, IOException {
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumnList() == null || hbaseData.getColumnList().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Scan scan = new Scan();
        if (hbaseData.getStartRow() != null)
            scan.setStartRow(Bytes.toBytes(hbaseData.getStartRow()));
        if (hbaseData.getStopRow() != null) {
            scan.setStopRow(Bytes.toBytes(hbaseData.getStopRow()));
        }
        if (hbaseData.getMaxTimestamp() != 0 && hbaseData.getMinTimestamp() != 0) {
            scan.setTimeRange(hbaseData.getMinTimestamp(), hbaseData.getMaxTimestamp());
        }
        byte[] colFam = Bytes.toBytes(hbaseData.getColFamily());
        for (String cols : hbaseData.getColumnList()) {
            scan.addColumn(colFam, Bytes.toBytes(cols));
        }
        ResultScanner resultList = this.hTable.getScanner(scan);
        try {
            HashMap<String, HashMap<String, String>> allData = new HashMap<String, HashMap<String, String>>();
            for (Result result : resultList) {
                HashMap<String, String> resultSet = new HashMap<String, String>(hbaseData.getColumnList().size());
                for (String cols : hbaseData.getColumnList()) {
                    if (result.containsColumn(colFam, Bytes.toBytes(cols))) {
                        resultSet.put(cols, Bytes.toString(result.getValue(colFam,
                                Bytes.toBytes(cols))));
                    }
                }
                allData.put(Bytes.toString(result.getRow()), resultSet);
            }
            return allData;
        } finally {
            resultList.close();
        }

    }

    public HashMap<String, HashMap<String, String>> MultiRowMultiColFetchWithColumnTimestamp(HbaseData hbaseData) throws
            HbaseApiException.ValidationError, IOException {
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumnList() == null || hbaseData.getColumnList().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Scan scan = new Scan();
        if (hbaseData.getStartRow() != null)
            scan.setStartRow(Bytes.toBytes(hbaseData.getStartRow()));
        if (hbaseData.getStopRow() != null) {
            scan.setStopRow(Bytes.toBytes(hbaseData.getStopRow()));
        }
        if (hbaseData.getMaxTimestamp() != 0 && hbaseData.getMinTimestamp() != 0) {
            scan.setTimeRange(hbaseData.getMinTimestamp(), hbaseData.getMaxTimestamp());
        }
        byte[] colFam = Bytes.toBytes(hbaseData.getColFamily());
        for (String cols : hbaseData.getColumnList()) {
            scan.addColumn(colFam, Bytes.toBytes(cols));
        }

        ResultScanner resultList = this.hTable.getScanner(scan);
        try {
            HashMap<String, HashMap<String, String>> allData = new HashMap<String, HashMap<String, String>>();
            for (Result result : resultList) {
                HashMap<String, String> resultSet = new HashMap<String, String>(hbaseData.getColumnList().size());
                for (String cols : hbaseData.getColumnList()) {
                    if (result.containsColumn(colFam, Bytes.toBytes(cols))) {
                        resultSet.put(cols + "+" + result.getColumnCells(colFam, Bytes.toBytes(cols)).get(0).getTimestamp(),
                                Bytes.toString(result.getValue(colFam, Bytes.toBytes(cols))));
                    }
                }
                allData.put(Bytes.toString(result.getRow()), resultSet);
            }
            return allData;
        } finally {
            resultList.close();
        }

    }

    public HashMap<String, HashMap<String, String>> MultiRowAllColFetchWithColumnFamily(HbaseData hbaseData) throws
            HbaseApiException.ValidationError, IOException {
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Scan scan = new Scan();
        if (hbaseData.getStartRow() != null)
            scan.setStartRow(Bytes.toBytes(hbaseData.getStartRow()));
        if (hbaseData.getStopRow() != null) {
            scan.setStopRow(Bytes.toBytes(hbaseData.getStopRow()));
        }
        byte[] colFam = Bytes.toBytes(hbaseData.getColFamily());
        scan.addFamily(colFam);

        ResultScanner resultList = this.hTable.getScanner(scan);

        try {
            HashMap<String, HashMap<String, String>> allData = new HashMap<String, HashMap<String, String>>();
            for(Result result:resultList){
                NavigableMap<byte[],byte[]> keyValue = result.getFamilyMap(Bytes.toBytes(hbaseData.getColFamily()));
                HashMap<String,String> resultSet = new HashMap<String, String>(result.size());
                for (NavigableMap.Entry<byte[],byte[]> row: keyValue.entrySet()){
                    resultSet.put(Bytes.toString(row.getKey()),Bytes.toString(row.getValue()));
                }
                allData.put(Bytes.toString(result.getRow()), resultSet);
            }
            return allData;
        } finally {
            resultList.close();
        }
    }

    public HashMap<String, String> SingleRowMultiColFetch(HbaseData hbaseData) throws HbaseApiException.ValidationError,
            IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumnList() == null || hbaseData.getColumnList().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Get get = new Get(Bytes.toBytes(hbaseData.getRow()));
        for (String cols : hbaseData.getColumnList()) {
            get.addColumn(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(cols));
        }
        Result result = this.hTable.get(get);
        this.setRowsCount(result.size());
        if (this.getRowsCount() >= 1) {
            HashMap<String, String> resultSet = new HashMap<String, String>(hbaseData.getColumnList().size());
            for (String cols : hbaseData.getColumnList()) {
                resultSet.put(cols, Bytes.toString(result.getValue(Bytes.toBytes(hbaseData.getColFamily()),
                        Bytes.toBytes(cols))));
            }
            return resultSet;
        }
        return new HashMap<String, String>(0);
    }
    public HashMap<String, String> SingleRowMultiColFetchWithColFam(HbaseData hbaseData) throws HbaseApiException.ValidationError,
            IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Get get = new Get(Bytes.toBytes(hbaseData.getRow()));
        get.addFamily(Bytes.toBytes(hbaseData.getColFamily()));
        Result result = this.hTable.get(get);
        this.setRowsCount(result.size());
        if (this.getRowsCount() >= 1) {
            HashMap<String, String> resultSet = new HashMap<String, String>(result.size());
            NavigableMap<byte[],byte[]>  keyValue = result.getFamilyMap(Bytes.toBytes(hbaseData.getColFamily()));
            for(NavigableMap.Entry<byte[], byte[]> row : keyValue.entrySet()){
                resultSet.put(Bytes.toString(row.getKey()), Bytes.toString(row.getValue()));
            }
            return resultSet;
        }
        return new HashMap<String, String>(0);
    }

    public void SingleRowMultiColInsert(HbaseData hbaseData) throws HbaseApiException.ValidationError,
            IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumnValuePair() == null || hbaseData.getColumnValuePair().size() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        for (Map.Entry<String, String> colValPair : hbaseData.getColumnValuePair().entrySet()) {
            put.add(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(colValPair.getKey()),
                    Bytes.toBytes(colValPair.getValue()));
        }
        this.hTable.put(put);
    }

    public void DeleteRowWithColumn(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumn() == null || hbaseData.getColumn().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Delete delete = new Delete(Bytes.toBytes(hbaseData.getRow()));
        delete.deleteColumns(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(hbaseData.getColumn()));
        this.hTable.delete(delete);
    }

    public void DeleteRowWithColumnFamily(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Delete delete = new Delete(Bytes.toBytes(hbaseData.getRow()));
        delete.deleteFamily(Bytes.toBytes(hbaseData.getColFamily()));
        this.hTable.delete(delete);
    }

    public void SingleRowColInsert(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumn() == null || hbaseData.getColumn().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        put.add(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(hbaseData.getColumn()),
                Bytes.toBytes(hbaseData.getValue()));
        this.hTable.put(put);
    }

    public void MultipleRowInsertInBatch(List<HbaseData> hbaseDatas) throws HbaseApiException.ValidationError, IOException {
        if (hbaseDatas == null )
            throw new HbaseApiException.ValidationError("Null List supplied");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        List<Put> puts = new ArrayList<>(hbaseDatas.size());
        for(HbaseData hbaseData : hbaseDatas) {
            if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
                throw new HbaseApiException.ValidationError("Invalid column family name");
            if (hbaseData.getColumn() == null || hbaseData.getColumn().length() == 0)
                throw new HbaseApiException.ValidationError("Invalid column name");
            Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
            put.add(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(hbaseData.getColumn()),
                    Bytes.toBytes(hbaseData.getValue()));
            puts.add(put);
        }
        this.hTable.put(puts);
    }


    public String SingleRowColFetch(HbaseData hbaseData) throws HbaseApiException.ValidationError, IOException {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (hbaseData.getColFamily() == null || hbaseData.getColFamily().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column family name");
        if (hbaseData.getColumn() == null || hbaseData.getColumn().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid column name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Get get = new Get(Bytes.toBytes(hbaseData.getRow()));
        get.addColumn(Bytes.toBytes(hbaseData.getColFamily()), Bytes.toBytes(hbaseData.getColumn()));
        Result result = this.hTable.get(get);
        this.setRowsCount(result.size());
        byte[] resultValue = result.getValue(Bytes.toBytes(hbaseData.getColFamily()),
                Bytes.toBytes(hbaseData.getColumn()));
        return Bytes.toString(resultValue);
    }

    private void _insert(HbaseData hbaseData) throws IOException, HbaseApiException.ValidationError {
        if (hbaseData.getRow() == null || hbaseData.getRow().length() == 0)
            throw new HbaseApiException.ValidationError("Invalid row name");
        if (this.hTable == null) {
            throw new HbaseApiException.ValidationError("Table instance not initialized");
        }
        Put put = new Put(Bytes.toBytes(hbaseData.getRow()));
        if (hbaseData.getRecord() == null || hbaseData.getRecord().isEmpty())
            throw new HbaseApiException.ValidationError("Invalid column family");
        for (Map.Entry<String, HashMap<String, String>> colFamily : hbaseData.getRecord().entrySet()) {
            for (Map.Entry<String, String> colVal : colFamily.getValue().entrySet()) {
                put.add(Bytes.toBytes(colFamily.getKey()),
                        Bytes.toBytes(colVal.getKey()), Bytes.toBytes(colVal.getValue()));
            }
        }
        this.hTable.put(put);
    }

    public void Close() throws IOException {
        if (this.hTable != null)
            this.hTable.close();
    }

    public void Insert(HbaseData hbaseData) throws IOException,
            HbaseApiException.EmptyTableName, HbaseApiException.ValidationError {
        if (hbaseData == null)
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
