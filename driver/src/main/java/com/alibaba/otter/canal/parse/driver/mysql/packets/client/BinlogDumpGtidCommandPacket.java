package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * COM_BINLOG_DUMP
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class BinlogDumpGtidCommandPacket extends CommandPacket {

    /** BINLOG_DUMP options */
    public static final int BINLOG_DUMP_NON_BLOCK           = 1;
    public static final int BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;
    public long             binlogPosition;
    public long             slaveServerId;
    public String           binlogFileName;
    private GtidSet         gtidSet;

    public BinlogDumpGtidCommandPacket(GtidSet gtidSet){
        setCommand((byte) 0x1E);
        this.gtidSet = gtidSet;
    }

    public void fromBytes(byte[] data) {
        // bypass
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            command
     *  n                            arg
     *  --------------------------------------------------------
     *  Bytes                        Name
     *  -----                        ----
     *  4                            binlog position to start at (little endian)
     *  2                            binlog flags (currently not used; always 0)
     *  4                            server_id of the slave (little endian)
     *  n                            binlog file name (optional)
     * 
     * </pre>
     */
    /*public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 0. write command number
        out.write(getCommand());
        // 1. write 4 bytes bin-log position to start at
        ByteHelper.writeUnsignedIntLittleEndian(binlogPosition, out);
        // 2. write 2 bytes bin-log flags
        int binlog_flags = 0;
        binlog_flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT;
        out.write(binlog_flags);
        out.write(0x00);
        // 3. write 4 bytes server id of the slave
        ByteHelper.writeUnsignedIntLittleEndian(this.slaveServerId, out);
        // 4. write bin-log file name if necessary
        if (StringUtils.isNotEmpty(this.binlogFileName)) {
            out.write(this.binlogFileName.getBytes());
        }
        return out.toByteArray();
    }
     private String fetchGtidPurged() throws IOException {
        channel.write(new QueryCommand("show global variables like 'gtid_purged'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length != 0) {
            return resultSet[0].getValue(1).toUpperCase();
        }
        return "";
    }
    */
    
    public byte[] toBytes() throws IOException {
        GTIDByteArrayOutputStream buffer = new GTIDByteArrayOutputStream();
        buffer.writeInteger(getCommand(), 1);
        buffer.writeInteger(0, 2); // flag
        buffer.writeLong(this.slaveServerId, 4);
        buffer.writeInteger(this.binlogFileName.length(), 4);
        buffer.writeString(this.binlogFileName);
        buffer.writeLong(this.binlogPosition, 8);
        Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        int dataSize = 8 /* number of uuidSets */;
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            dataSize += 16 /* uuid */ + 8 /* number of intervals */ +
                uuidSet.getIntervals().size() /* number of intervals */ * 16 /* start-end */;
        }
        buffer.writeInteger(dataSize, 4);
        buffer.writeLong(uuidSets.size(), 8);
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            buffer.write(hexToByteArray(uuidSet.getUUID().replace("-", "")));
            Collection<GtidSet.Interval> intervals = uuidSet.getIntervals();
            buffer.writeLong(intervals.size(), 8);
            for (GtidSet.Interval interval : intervals) {
                buffer.writeLong(interval.getStart(), 8);
                buffer.writeLong(interval.getEnd() + 1 /* right-open */, 8);
            }
        }
        return buffer.toByteArray();
    }

    private static byte[] hexToByteArray(String uuid) {
        byte[] b = new byte[uuid.length() / 2];
        for (int i = 0, j = 0; j < uuid.length(); j += 2) {
            b[i++] = (byte) Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16);
        }
        return b;
    }

}
class GTIDByteArrayOutputStream extends OutputStream {

    private OutputStream outputStream;

    public GTIDByteArrayOutputStream() {
        this(new java.io.ByteArrayOutputStream());
    }

    public GTIDByteArrayOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    /**
     * Write int in little-endian format.
     */
    public void writeInteger(int value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            write(0x000000FF & (value >>> (i << 3)));
        }
    }

    /**
     * Write long in little-endian format.
     */
    public void writeLong(long value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            write((int) (0x00000000000000FF & (value >>> (i << 3))));
        }
    }

    public void writeString(String value) throws IOException {
        write(value.getBytes());
    }

    /**
     * @see ByteArrayInputStream#readZeroTerminatedString()
     */
    public void writeZeroTerminatedString(String value) throws IOException {
        write(value.getBytes());
        write(0);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    public byte[] toByteArray() {
        // todo: whole approach feels wrong
        if (outputStream instanceof java.io.ByteArrayOutputStream) {
            return ((java.io.ByteArrayOutputStream) outputStream).toByteArray();
        }
        return new byte[0];
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

}
