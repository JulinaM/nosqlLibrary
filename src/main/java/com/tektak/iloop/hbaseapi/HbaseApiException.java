package com.tektak.iloop.hbaseapi;

/**
 * Created by Dipak Malla
 * Date: 7/25/14
 */
public interface HbaseApiException {
    public class EmptyTableName extends Exception {
        public EmptyTableName(String msg) {
            super(msg);
        }

        public EmptyTableName(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public class ValidationError extends Exception {
        public ValidationError(String msg) {
            super(msg);
        }

        public ValidationError(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public class EmptyRecord extends Exception {
        public EmptyRecord(String msg) {
            super(msg);
        }

        public EmptyRecord(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public class TableInvalid extends Exception {
        public TableInvalid(String msg) {
            super(msg);
        }

        public TableInvalid(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public class HbaseDataNull extends Exception {
        public HbaseDataNull(String msg) {
            super(msg);
        }

        public HbaseDataNull(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }
}
