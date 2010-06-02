package org.apache.activemq.store.cassandra;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.cassandra.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.*;
import java.util.Collections;


public class CassandraUtils {

    static Logger log = LoggerFactory.getLogger(CassandraUtils.class);

    public static String getString(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }

    public static long getLong(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        LongBuffer lBuffer = byteBuffer.asLongBuffer();
        return lBuffer.get();
    }

    public static byte[] getBytes(long num) {
        byte[] bArray = new byte[8];
        ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
        LongBuffer lBuffer = bBuffer.asLongBuffer();
        lBuffer.put(num);
        return bArray;
    }

    public static int getInt(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        IntBuffer iBuffer = byteBuffer.asIntBuffer();
        return iBuffer.get();
    }

    public static byte[] getBytes(int num) {
        byte[] bArray = new byte[4];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bArray);
        IntBuffer iBuffer = byteBuffer.asIntBuffer();
        iBuffer.put(num);
        return bArray;
    }


    public static long safeGetLong(byte[] bytes) {
        if (bytes.length != 8) {
            log.debug("bytes length was {}, not 8, returning -1", bytes.length);
            return -1L;
        } else {
            return getLong(bytes);
        }
    }

    public static boolean getBoolean(byte[] bytes) {
        return Boolean.parseBoolean(getString(bytes));
    }


    public static byte[] getBytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return empty byte array if string is null or ""
     *
     * @param string
     * @return
     */
    public static byte[] nullSafeGetBytes(String string) {
        if (string == null || "".equals(string)) {
            return new byte[0];
        } else {
            return getBytes(string);
        }
    }


    private static byte[] getBytes(boolean bool) {
        return getBytes(Boolean.toString(bool));
    }


    public static long timestamp() {
        return System.currentTimeMillis();
    }

    public static Mutation getInsertOrUpdateColumnMutation(byte[] name, byte[] val, Long timestamp) {
        if (timestamp == null) {
            timestamp = timestamp();
        }
        Mutation insert = new Mutation();
        ColumnOrSuperColumn c = new ColumnOrSuperColumn();
        c.setColumn(getColumn(name, val, timestamp));
        insert.setColumn_or_supercolumn(c);
        return insert;
    }

    public static Mutation getDeleteColumnMutation(byte[] column, long timestamp) {
        Mutation mutation = new Mutation();
        Deletion deletion = new Deletion(timestamp);
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(Collections.singletonList(column));
        deletion.setPredicate(predicate);
        mutation.setDeletion(deletion);
        return mutation;
    }

   

    public static Mutation getInsertOrUpdateColumnMutation(String name, String val) {
        return getInsertOrUpdateColumnMutation(getBytes(name), getBytes(val), null);
    }

    public static Mutation getInsertOrUpdateColumnMutation(String name, long val) {
        return getInsertOrUpdateColumnMutation(getBytes(name), getBytes(val), null);
    }

    public static Mutation getInsertOrUpdateColumnMutation(String name, boolean val) {
        return getInsertOrUpdateColumnMutation(getBytes(name), getBytes(Boolean.toString(val)), null);
    }

    public static Column getColumn(byte[] name, byte[] val) {
        return new Column(name, val, timestamp());
    }

    public static Column getColumn(byte[] name, byte[] val, long timestamp) {
        return new Column(name, val, timestamp);
    }

    public static Mutation getInsertOrUpdateSuperColumnMutation(long supername, String name, byte[] val) {
        return getInsertOrUpdateSuperColumnMutation(getBytes(supername), getBytes(name), val);
    }

    public static Mutation getInsertOrUpdateSuperColumnMutation(byte[] supername, byte[] name, byte[] val) {
        Mutation mutation = new Mutation();
        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        SuperColumn superColumn = new SuperColumn();
        superColumn.setName(supername);
        Column column = getColumn(name, val);
        superColumn.addToColumns(column);
        columnOrSuperColumn.setSuper_column(superColumn);
        mutation.setColumn_or_supercolumn(columnOrSuperColumn);
        return mutation;
    }

    public static String getDestinationKey(ActiveMQDestination destination) {
        String key = destination.getQualifiedName();
        return key;
    }
}
