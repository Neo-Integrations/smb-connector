package org.neointegrations.smb.internal.transport;

import com.hierynomus.protocol.PacketData;
import com.hierynomus.protocol.commons.buffer.Buffer;
import com.hierynomus.protocol.commons.buffer.Endian;
import com.hierynomus.protocol.transport.PacketFactory;
import com.hierynomus.protocol.transport.PacketReceiver;
import com.hierynomus.protocol.transport.TransportException;
import com.hierynomus.smbj.transport.PacketReader;
import org.mule.runtime.api.connection.ConnectionException;
import org.neointegrations.smb.internal.SMBConnection;
import org.neointegrations.smb.internal.SMBConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

public class CustomDirectTcpPacketReader<D extends PacketData<?>> extends PacketReader<D> {
    private final Logger _logger = LoggerFactory.getLogger(this.getClass());
    private final PacketFactory<D> packetFactory;
    private final SMBConnectionProvider _connectionProvider;
    private final SMBConnection _connection;

    public CustomDirectTcpPacketReader(final String host, final InputStream in,
                                       final PacketFactory<D> packetFactory,
                                       final PacketReceiver<D> handler,
                                       final SMBConnectionProvider connectionProvider,
                                       final SMBConnection connection) {
        super(host, in, handler);
        this.packetFactory = packetFactory;
        this._connectionProvider = connectionProvider;
        this._connection = connection;
    }

    private D readPacket(int packetLength) throws IOException, Buffer.BufferException {
        byte[] buf = new byte[packetLength];
        readFully(buf);
        return packetFactory.read(buf);
    }

    @Override
    protected D doRead() throws TransportException {
        try {
            int packetLength = readTcpHeader();
            return readPacket(packetLength);
        } catch (TransportException e) {
            throw e;
        } catch (IOException | Buffer.BufferException e) {
            _logger.error("Connection issue", e.getMessage(), e);
            if(e instanceof SocketTimeoutException) {
                try {
                    this._connectionProvider.reconnect(this._connection);
                } catch (ConnectionException ex) {
                    ex.printStackTrace();
                }
            }
            throw new TransportException(e);
        }
    }

    private int readTcpHeader() throws IOException, Buffer.BufferException {
        byte[] tcpHeader = new byte[4];
        readFully(tcpHeader);
        Buffer.PlainBuffer plainBuffer = new Buffer.PlainBuffer(tcpHeader, Endian.BE);
        plainBuffer.readByte();
        int packetLength = plainBuffer.readUInt24();
        return packetLength;
    }

    private void readFully(byte[] buffer) throws IOException {
        int toRead = buffer.length;
        int offset = 0;
        while (toRead > 0) {
            int bytesRead = in.read(buffer, offset, toRead);
            if (bytesRead == -1) {
                throw new TransportException(new EOFException("EOF while reading packet"));
            }
            toRead -= bytesRead;
            offset += bytesRead;
        }
    }
}
