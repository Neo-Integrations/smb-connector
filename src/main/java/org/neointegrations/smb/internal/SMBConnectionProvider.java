package org.neointegrations.smb.internal;

import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.common.SMBRuntimeException;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.api.connection.PoolingConnectionProvider;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.connection.CachedConnectionProvider;


import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This class (as it's name implies) provides connection instances and the funcionality to disconnect and validate those
 * connections.
 * <p>
 * All connection related parameters (values required in order to create a connection) must be
 * declared in the connection providers.
 * <p>
 * This particular example is a {@link PoolingConnectionProvider} which declares that connections resolved by this provider
 * will be pooled and reused. There are other implementations like {@link CachedConnectionProvider} which lazily creates and
 * caches connections or simply {@link ConnectionProvider} if you want a new connection each time something requires one.
 */
public class SMBConnectionProvider implements PoolingConnectionProvider<SMBConnection> {
    private final Logger _logger = LoggerFactory.getLogger(SMBConnectionProvider.class);

    @Optional(defaultValue = "user")
    @Parameter
    private String user;

    @Optional(defaultValue = "password")
    @Password
    @Parameter
    private String password;

    @Optional(defaultValue = "localhost")
    @Parameter
    private String host;

    @Optional(defaultValue = "445")
    @Parameter
    private int port;

    @Parameter
    @Optional(defaultValue = "WORKGROUP")
    private String domain;

    @Parameter
    private String share;

    @Optional( defaultValue = "60")
    @Parameter
    private long timeout;

    @Optional(defaultValue = "3600")
    @Parameter
    private long socketTimeout;

    private SmbConfig _smbConfig = null;


    @Override
    public SMBConnection connect() throws ConnectionException {
        if(_logger.isDebugEnabled())
            _logger.debug("*** Opening connection ");
        if(_logger.isDebugEnabled())
            _logger.debug("Connecting with Share [{}], domain [{}] ",new Object[]{this.share,this.domain});

        if (this.share == null || Constant.EMPTY.equals(this.share)) {
            throw new IllegalArgumentException("Share name can't be empty or null");
        }

        try {
            if (this.share.startsWith("/")) {
                this.share = this.share.replace("/", Constant.EMPTY).trim();
            }
            SMBConnection connection = new SMBConnection();
            SAMBAClient smbClient = new SAMBAClient(this, connection, this.timeout, this.socketTimeout);
            Session session = smbClient.login(this.host, this.port,
                    this.user, this.password, this.domain);
            DiskShare diskShare = (DiskShare) session.connectShare(this.share);

            _logger.info("*** Opening connection done ");
            return connection.set(smbClient, diskShare, this, session);

        } catch (IOException exp) {
            _logger.error("Unable to make connection to the SMB server [{}] ", exp.getMessage(), exp);
        } catch (SMBRuntimeException exp) {
            _logger.error("Authentication failure [{}] ", exp.getMessage(), exp);
        } catch(Exception exp) {
            _logger.error("Connection request failed [{}] ", exp.getMessage(), exp);
        }

        _logger.info("*** Opening connection error ");
        throw new ConnectionException("Unable to make connection to the SMB server");
    }


    @Override
    public void disconnect(SMBConnection connection) {
        connection.getSmbClient().disconnect(connection);
    }

    @Override
    public ConnectionValidationResult validate(SMBConnection connection) {
        ConnectionValidationResult status = null;
        _logger.info(" *** Connection Status {} ", connection.getDiskShare().isConnected());
        if (connection.getDiskShare().isConnected()) {
            status = ConnectionValidationResult.success();
        } else {
            status = ConnectionValidationResult.failure("Stale connection",
                    new RuntimeException("Unable to validate the connection"));
        }
        return status;
    }

    public void reconnect(SMBConnection connection) throws ConnectionException {
        _logger.info(" *** Reconnecting...");
        try {
            connection.getSmbClient().disconnect(connection);
            final SAMBAClient smbClient = new SAMBAClient(this, connection, this.timeout, this.socketTimeout);
            final Session session = smbClient.login(this.host, this.port,
                    this.user, this.password, this.domain);
            final DiskShare share = (DiskShare) session.connectShare(this.share);
            connection.set(smbClient, share, this, session);
        } catch(Exception exp) {
            _logger.error("Unable to reconnect [{}] ", exp.getMessage(), exp);
            throw new ConnectionException(exp);
        }
        _logger.info(" *** Reconnecting done");
    }
}
