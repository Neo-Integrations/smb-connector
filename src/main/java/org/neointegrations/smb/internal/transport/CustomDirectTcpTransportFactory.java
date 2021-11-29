package org.neointegrations.smb.internal.transport;


import com.hierynomus.protocol.Packet;
import com.hierynomus.protocol.PacketData;
import com.hierynomus.protocol.transport.PacketHandlers;
import com.hierynomus.protocol.transport.TransportLayer;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.transport.TransportLayerFactory;
import org.neointegrations.smb.internal.SMBConnection;
import org.neointegrations.smb.internal.SMBConnectionProvider;


public class CustomDirectTcpTransportFactory<D extends PacketData<?>, P extends Packet<?>> implements TransportLayerFactory<D, P> {

    private final SMBConnectionProvider _connectionProvider;
    private final SMBConnection _connection;

    public CustomDirectTcpTransportFactory(final SMBConnectionProvider connectionProvider, final SMBConnection connection) {
        super();
        this._connectionProvider = connectionProvider;
        this._connection = connection;
    }

    @Override
    public TransportLayer<P> createTransportLayer(PacketHandlers<D, P> handlers, SmbConfig config) {
        return new CustomDirectTcpTransport(config.getSocketFactory(),
                config.getSoTimeout(),
                handlers,
                this._connectionProvider,
                this._connection);
    }

}