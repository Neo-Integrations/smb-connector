package org.neointegrations.smb.internal;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smb.SMBPacket;
import com.hierynomus.smb.SMBPacketData;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.File;
import com.hierynomus.smbj.transport.TransportLayerFactory;
import org.neointegrations.smb.internal.transport.CustomDirectTcpTransportFactory;
import org.neointegrations.smb.internal.util.SMBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SAMBAClient extends SMBClient {

    private final Logger _logger = LoggerFactory.getLogger(SAMBAClient.class);

    public SAMBAClient(SMBConnectionProvider provider,
                       SMBConnection connection, long timeout, long socketTimeout) {
        super(getConfig(provider, connection, timeout, socketTimeout));
    }

    public Session  login(String host, int port, String user, String password, String domain) throws IOException {
        Connection connect = this.connect(host, port);
        AuthenticationContext authenticationContext =
                new AuthenticationContext(user,
                        password.toCharArray(),
                        domain);

        return connect.authenticate(authenticationContext);
    }

    private static SmbConfig getConfig(SMBConnectionProvider provider,
                               SMBConnection connection, long timeout, long socketTimeout) {

        final TransportLayerFactory<SMBPacketData<?>, SMBPacket<?, ?>> DEFAULT_TRANSPORT_LAYER_FACTORY =
                new CustomDirectTcpTransportFactory(provider, connection);

        return SmbConfig.builder()
                .withTimeout(timeout, TimeUnit.SECONDS)
                .withSoTimeout(socketTimeout, TimeUnit.SECONDS)
                .withTransportLayerFactory(DEFAULT_TRANSPORT_LAYER_FACTORY)
                .withDialects(SMB2Dialect.SMB_3_1_1, SMB2Dialect.SMB_3_0_2, SMB2Dialect.SMB_3_0, SMB2Dialect.SMB_2_1, SMB2Dialect.SMB_2_0_2)
                .build();
    }

    public void disconnect(SMBConnection connection) {
        _logger.info("*** Closing connection ");
        if (connection.getDiskShare() != null) {
            SMBUtil.close(connection.getDiskShare());
        }
        if (connection.getSession() != null) {
            SMBUtil.close(connection.getSession());
            SMBUtil.close(connection.getSession().getConnection());
        }
        if (connection.getSmbClient() != null) {
            SMBUtil.close(connection.getSmbClient());
        }
        _logger.info("*** Closing connection done ");
    }

    public File openFileForRead(final String fileName,
                         final String folder,
                         final SMBConnection smbConnection,
                         final boolean readLock) {

        Set<AccessMask> accessMasks = new HashSet();
        accessMasks.add(AccessMask.MAXIMUM_ALLOWED);
        Set<FileAttributes> fileAttributes = new HashSet();
        fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_READONLY);
        Set<SMB2ShareAccess> shareAccesses = new HashSet();
        if (!readLock) {
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_READ);
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_WRITE);
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_DELETE);
        }

        Set<SMB2CreateOptions> smb2CreateOptions = new HashSet();
        smb2CreateOptions.add(SMB2CreateOptions.FILE_RANDOM_ACCESS);
        String dest = SMBUtil.prepareFilePath(folder, fileName);

        return smbConnection.getDiskShare().openFile(dest, accessMasks,
                fileAttributes, shareAccesses,
                SMB2CreateDisposition.FILE_OPEN, smb2CreateOptions);
    }

    public File openFileForWrite(String fileName,
                          String targetFolder,
                          SMBConnection smbConnection,
                          boolean fileOverwrite,
                          boolean append,
                          boolean writeLock) {
        Set<AccessMask> accessMasks = new HashSet();
        accessMasks.add(AccessMask.MAXIMUM_ALLOWED);

        Set<FileAttributes> fileAttributes = new HashSet();
        fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_NORMAL);

        Set<SMB2ShareAccess> shareAccesses = new HashSet();
        if (writeLock) {
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_READ);
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_WRITE);
            shareAccesses.add(SMB2ShareAccess.FILE_SHARE_DELETE);
        }

        Set<SMB2CreateOptions> smb2CreateOptions = new HashSet();
        smb2CreateOptions.add(SMB2CreateOptions.FILE_RANDOM_ACCESS);

        if (!smbConnection.getDiskShare().folderExists(targetFolder)) {
            _logger.info("Destination directory doesn't exist, creating new directory");
            smbConnection.getDiskShare().mkdir(targetFolder);
        }

        String dest = SMBUtil.prepareFilePath(targetFolder, fileName);
        if (fileOverwrite) {
            _logger.info("If the file {} already exists, will be overritten ", fileName);
            return smbConnection.getDiskShare().openFile(dest, accessMasks, fileAttributes,
                    shareAccesses, SMB2CreateDisposition.FILE_OVERWRITE_IF, smb2CreateOptions);
        } else {
            _logger.info("File name {} will be created ", fileName);
            return append ?
                    smbConnection.getDiskShare().openFile(dest, accessMasks, fileAttributes,
                            shareAccesses, SMB2CreateDisposition.FILE_OPEN_IF, smb2CreateOptions) :
                    smbConnection.getDiskShare().openFile(dest, accessMasks, fileAttributes, shareAccesses,
                            SMB2CreateDisposition.FILE_CREATE, smb2CreateOptions);
        }
    }
}
