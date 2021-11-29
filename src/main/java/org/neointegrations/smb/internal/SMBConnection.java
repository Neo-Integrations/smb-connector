package org.neointegrations.smb.internal;


import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.connection.ConnectionValidationResult;

/**
 * This class represents an extension connection just as example (there is no real connection with anything here c:).
 */
public class SMBConnection {

  private SAMBAClient smbClient;
  private DiskShare diskShare;
  private SMBConnectionProvider provider;
  private Session _session;

  public SMBConnection() {}

  public SMBConnection(final SAMBAClient smbClient,
                       final DiskShare diskShare,
                       final SMBConnectionProvider provider,
                       final Session session) {
    this.smbClient = smbClient;
    this.diskShare = diskShare;
    this.provider = provider;
    this._session = session;
  }

  public SMBConnection set(final SAMBAClient smbClient,
                  final DiskShare diskShare,
                  final SMBConnectionProvider provider,
                  final Session session) {
    this.smbClient = smbClient;
    this.diskShare = diskShare;
    this.provider = provider;
    this._session = session;
    return this;
  }

  public void close() {
    provider.disconnect(this);
  }

  public SAMBAClient getSmbClient() {
    return smbClient;
  }

  public DiskShare getDiskShare() {
    return diskShare;
  }

  public SMBConnectionProvider getProvider() {
    return provider;
  }

  public Session getSession() {
    return _session;
  }


}
