package org.neointegrations.smb.internal.transport;

import com.hierynomus.protocol.Packet;
import com.hierynomus.protocol.PacketData;
import com.hierynomus.protocol.commons.buffer.Buffer;
import com.hierynomus.protocol.commons.socket.ProxySocketFactory;
import com.hierynomus.protocol.transport.PacketHandlers;
import com.hierynomus.protocol.transport.TransportException;
import com.hierynomus.protocol.transport.TransportLayer;
import com.hierynomus.smbj.transport.PacketReader;
import org.neointegrations.smb.internal.SMBConnection;
import org.neointegrations.smb.internal.SMBConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

public class CustomDirectTcpTransport<D extends PacketData<?>, P extends Packet<?>> implements TransportLayer<P> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final PacketHandlers<D, P> handlers;

    private final ReentrantLock writeLock = new ReentrantLock();

    private SocketFactory socketFactory = new ProxySocketFactory();
    private int soTimeout;

    private Socket socket;
    private BufferedOutputStream output;
    private PacketReader<D> packetReaderThread;

    private static final int INITIAL_BUFFER_SIZE = 9000;

    private final SMBConnectionProvider _connectionProvider;
    private final SMBConnection _connection;

    public CustomDirectTcpTransport(final SocketFactory socketFactory,
                                    final int soTimeout,
                                    final PacketHandlers<D, P> handlers,
                                    final SMBConnectionProvider connectionProvider,
                                    final SMBConnection connection) {
        this.soTimeout = soTimeout;
        this.socketFactory = socketFactory;
        this.handlers = handlers;
        this._connectionProvider = connectionProvider;
        this._connection = connection;
    }

    @Override
    public void write(P packet) throws TransportException {
        logger.trace("Acquiring write lock to send packet << {} >>", packet);
        writeLock.lock();
        try {
            if (!isConnected()) {
                throw new TransportException(format("Cannot write %s as transport is disconnected", packet));
            }
            try {
                logger.debug("Writing packet {}", packet);
                Buffer<?> packetData = handlers.getSerializer().write(packet);
                writeDirectTcpPacketHeader(packetData.available());
                writePacketData(packetData);
                output.flush();
                logger.trace("Packet {} sent, lock released.", packet);
            } catch (IOException ioe) {
                throw new TransportException(ioe);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void connect(InetSocketAddress remoteAddress) throws IOException {
        String remoteHostname = remoteAddress.getHostString();
        this.socket = socketFactory.createSocket(remoteHostname, remoteAddress.getPort());
        initWithSocket(remoteHostname);
    }

    private void initWithSocket(String remoteHostname) throws IOException {
        this.socket.setSoTimeout(soTimeout);
        this.output = new BufferedOutputStream(this.socket.getOutputStream(), INITIAL_BUFFER_SIZE);
        packetReaderThread = new CustomDirectTcpPacketReader<>(remoteHostname,
                socket.getInputStream(),
                handlers.getPacketFactory(),
                handlers.getReceiver(),
                this._connectionProvider,
                this._connection);
        packetReaderThread.start();
    }


    @Override
    public void disconnect() throws IOException {
        writeLock.lock();
        try {
            if (!isConnected()) {
                return;
            }

            packetReaderThread.stop();
            if (socket.getInputStream() != null) {
                socket.getInputStream().close();
            }
            if (output != null) {
                output.close();
                output = null;
            }
            if (socket != null) {
                socket.close();
                socket = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isConnected() {
        return (socket != null) && socket.isConnected() && !socket.isClosed();
    }

    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    private void writePacketData(Buffer<?> packetData) throws IOException {
        output.write(packetData.array(), packetData.rpos(), packetData.available());
    }

    private void writeDirectTcpPacketHeader(int size) throws IOException {
        output.write(0);
        output.write((byte) (size >> 16));
        output.write((byte) (size >> 8));
        output.write((byte) (size & 0xFF));
    }
}
