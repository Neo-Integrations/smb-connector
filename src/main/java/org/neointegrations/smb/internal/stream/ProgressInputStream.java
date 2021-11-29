package org.neointegrations.smb.internal.stream;


import java.io.IOException;
import java.io.InputStream;


import com.hierynomus.smbj.share.File;
import org.neointegrations.smb.internal.SMBConnection;

public class ProgressInputStream extends InputStream {
    private final SMBConnection _smbConnection;
    private final InputStream _inputStream;
    private final File _file;
    private boolean _finished = false;
    private final boolean _deleteOnFinished;

    public ProgressInputStream(final SMBConnection smbConnection,
                               final InputStream inputStream,
                               final File file,
                               final boolean deleteOnFinished) {
        this._smbConnection = smbConnection;
        this._inputStream = inputStream;
        this._file = file;
        this._deleteOnFinished = deleteOnFinished;
    }

    @Override
    public void close() throws IOException {
        if(_inputStream != null) _inputStream.close();
        if(_file != null) {
            if(_finished && _deleteOnFinished) {
                _file.deleteOnClose();
            }
            _file.close();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        return _inputStream.skip(n);
    }

    @Override
    public int read() throws IOException {
        int count = this._inputStream.read();
        if(count == -1) _finished = true;
        return count;
    }


    @Override
    public int read(byte[] b) throws IOException {
        int count = this._inputStream.read();
        if(count == -1) _finished = true;
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int count = this._inputStream.read(b, off, len);
        if(count == -1) _finished = true;
        return count;
    }

    @Override
    public int available() throws IOException {
        return _inputStream.available();
    }

    @Override
    public synchronized void reset() throws IOException {
        _inputStream.reset();
    }

    @Override
    public synchronized void mark(int readlimit) {
        _inputStream.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return _inputStream.markSupported();
    }
}
