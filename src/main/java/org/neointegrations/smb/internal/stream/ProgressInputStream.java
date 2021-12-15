package org.neointegrations.smb.internal.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.concurrent.locks.ReentrantLock;


import com.hierynomus.smbj.share.File;
import org.neointegrations.smb.internal.SMBConnection;
import org.neointegrations.smb.internal.util.SMBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressInputStream extends InputStream {

    private static final Logger _logger = LoggerFactory.getLogger(ProgressInputStream.class);
    private final ReentrantLock _lock = new ReentrantLock();

    private InputStream _inputStream;
    private File _file;
    private boolean _finished = false;
    private final boolean _deleteOnFinished;
    private final boolean _createIntermediateFile;
    private final SMBConnection _smbConnection;
    private final boolean _lockTheFileWhileReading;

    public ProgressInputStream(final SMBConnection smbConnection,
                               final boolean lockTheFileWhileReading,
                               final File file,
                               final boolean deleteOnFinished,
                               final boolean createIntermediateFile) {
        this._smbConnection = smbConnection;
        this._lockTheFileWhileReading = lockTheFileWhileReading;
        this._file = file;
        this._deleteOnFinished = deleteOnFinished;
        this._createIntermediateFile = createIntermediateFile;
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
        if(_inputStream == null) lazyLoadStream();
        return _inputStream.skip(n);
    }

    @Override
    public int read() throws IOException {
        if(_inputStream == null) lazyLoadStream();
        int count = this._inputStream.read();
        if(count == -1) _finished = true;
        return count;
    }


    @Override
    public int read(byte[] b) throws IOException {
        if(_inputStream == null) lazyLoadStream();
        int count = this._inputStream.read();
        if(count == -1) _finished = true;
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(_inputStream == null) lazyLoadStream();
        int count = this._inputStream.read(b, off, len);
        if(count == -1) _finished = true;
        return count;
    }

    @Override
    public int available() throws IOException {
        if(_inputStream == null) lazyLoadStream();
        return _inputStream.available();
    }

    @Override
    public synchronized void reset() throws IOException {
        if(_inputStream == null) lazyLoadStream();
        _inputStream.reset();
    }

    @Override
    public synchronized void mark(int readlimit) {
        if(_inputStream == null) lazyLoadStream();
        _inputStream.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        if(_inputStream == null) lazyLoadStream();
        return _inputStream.markSupported();
    }

    private void lazyLoadStream() {
        if(_inputStream != null) return;

        _lock.lock();

        if(_createIntermediateFile == true) {
            String intermediateFileName = "__" + Calendar.getInstance().getTimeInMillis() + "_" + SMBUtil.fileName(_file.getPath());
            _file.rename(SMBUtil.trimPath(SMBUtil.directory(_file.getPath()), intermediateFileName));
            try{
                _file.close();
            } catch(Exception ignored) {
                _logger.error("Unable to close the file {}", ignored.getMessage(), ignored);
            }
            _file = _smbConnection.getSmbClient().openFileForRead(intermediateFileName,
                    SMBUtil.directory(_file.getPath()), _smbConnection, _lockTheFileWhileReading);

            _inputStream = _file.getInputStream();
        }
        _inputStream = _file.getInputStream();
        if(_logger.isDebugEnabled()) _logger.debug("File stream opened {}", SMBUtil.fileName(_file.getUncPath()));

        _lock.unlock();
    }
}
