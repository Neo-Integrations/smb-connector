package org.neointegrations.smb.internal.stream;

import com.hierynomus.smbj.share.File;
import org.mule.extension.file.common.api.AbstractFileAttributes;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.neointegrations.smb.internal.util.SMBUtil;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Date;

public class SMBFileAttributes extends AbstractFileAttributes {

    @Parameter
    private long size;

    @Parameter
    private boolean regularFile;

    @Parameter
    private boolean directory;

    @Parameter
    private boolean symbolicLink;

    @Parameter
    private String path;

    @Parameter
    private String name;

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Parameter
    private LocalDateTime timestamp;


    public SMBFileAttributes(long size, boolean regularFile, boolean directory,
                             boolean symbolicLink, String path, String name, Date timestamp) {
        super(Paths.get(SMBUtil.trimPath(path, name)));
        this.size = size;
        this.regularFile = regularFile;
        this.directory = directory;
        this.symbolicLink = symbolicLink;
        this.path = path;
        this.name = name;
        this.timestamp = asDateTime(timestamp.toInstant());
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setRegularFile(boolean regularFile) {
        this.regularFile = regularFile;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public void setSymbolicLink(boolean symbolicLink) {
        this.symbolicLink = symbolicLink;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public long getSize() {
        return this.size;
    }

    @Override
    public boolean isRegularFile() {
        return this.regularFile;
    }

    @Override
    public boolean isDirectory() {
        return this.directory;
    }

    @Override
    public boolean isSymbolicLink() {
        return this.symbolicLink;
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
