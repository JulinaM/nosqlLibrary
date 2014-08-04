package com.tektak.iloop.hbaseapi;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Dipak Malla
 * Date: 7/25/14
 */
public class HbaseData {
    private String table;
    private String row;
    private String colFamily;
    private ArrayList<String> columnList;
    private String column;
    private String value;
    private String startRow;
    private String stopRow;
    private HashMap<String, String> columnValuePair;
    private HashMap<String, HashMap<String, String>> record;

    public String getTable() {
        return table;
    }

    public void setTable(String table) throws HbaseApiException.EmptyTableName {
        if(table == null || table.length() == 0)
            throw new HbaseApiException.EmptyTableName("Empty or null table");
        this.table = table;
    }

    public String getRow() {
        return row;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public HashMap<String, HashMap<String, String>> getRecord() {
        return record;
    }

    public void setRecord(HashMap<String, HashMap<String, String>> record) throws HbaseApiException.EmptyRecord {
        if(record == null)
            throw new HbaseApiException.EmptyRecord("Record is null");
        if(record.isEmpty())
            throw  new HbaseApiException.EmptyRecord("Record is empty");
        if(record.entrySet().isEmpty())
            throw  new HbaseApiException.EmptyRecord("Record is empty");
        this.record = record;
    }

    public String getColFamily() {
        return colFamily;
    }

    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    public ArrayList<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(ArrayList<String> columnList) {
        this.columnList = columnList;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public HashMap<String, String> getColumnValuePair() {
        return columnValuePair;
    }

    public void setColumnValuePair(HashMap<String, String> columnValuePair) {
        this.columnValuePair = columnValuePair;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public String getStopRow() {
        return stopRow;
    }

    public void setStopRow(String stopRow) {
        this.stopRow = stopRow;
    }
}
