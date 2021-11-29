package org.neointegrations.smb.internal;

import org.mule.extension.file.common.api.FileConnectorConfig;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

/**
 * This class represents an extension configuration, values set in this class are commonly used across multiple
 * operations since they represent something core from the extension.
 */
@Operations({SMBReadOperations.class, SMBWriteOperations.class})
@ConnectionProviders(SMBConnectionProvider.class)
@Sources({SMBSources.class})
public class SMBConfiguration extends FileConnectorConfig {
  public SMBConfiguration() {
  }
}
