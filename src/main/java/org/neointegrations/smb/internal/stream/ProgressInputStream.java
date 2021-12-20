package org.neointegrations.smb.internal.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;


import com.hierynomus.smbj.share.File;
import org.neointegrations.smb.internal.SMBConnection;
import org.neointegrations.smb.internal.util.SMBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressInputStream extends InputStream {

    private static final Logger _logger = LoggerFactory.getLogger(ProgressInputStream.class);

    private InputStream _inputStream;
    private File _file;

    private String _fileName;
    private final String _originalFileName;
    private final String _directory;

    private boolean _finished = false;
    private boolean _started = false;
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
        this._originalFileName = SMBUtil.fileName(file.getPath());
        this._fileName = SMBUtil.fileName(file.getPath());
        this._directory = SMBUtil.directory(file.getPath());
        this._deleteOnFinished = deleteOnFinished;
        this._createIntermediateFile = createIntermediateFile;
    }

    @Override
    public void close() throws IOException {
        if(_inputStream != null) _inputStream.close();
        if(_file != null) {
            if ((_started == true && _finished == true) &&
                    _deleteOnFinished == true) {
                // Delete the file, if
                // - the transfer has been finished successfully,
                // - the _deleteTheFileAfterRead == true and
                // - the connection object was for the file.
                _file.deleteOnClose();
            } else if ((_started == true && _finished == false) &&
                    _fileName != _originalFileName) {
                // Rename to the original file name if
                // - the file was renamed to intermediate name
                // - the transfer was started but did not finished
                renameToIntermediateOrOriginal(false);
            }
            _file.close();
        }
        if(_logger.isDebugEnabled()) {
            _logger.debug("_started={} _finished={} _deleteOnFinished={}", _started, _finished, _deleteOnFinished);
            _logger.debug("_fileName={} _originalFileName={} ", _fileName, _originalFileName);
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
        if (_started == false && count >= 0) _started = true;
        else if(count == -1) _finished = true;
        return count;
    }


    @Override
    public int read(byte[] b) throws IOException {
        if(_inputStream == null) lazyLoadStream();
        int count = this._inputStream.read();
        if (_started == false && count >= 0) _started = true;
        else if(count == -1) _finished = true;
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(_inputStream == null) lazyLoadStream();
        int count = this._inputStream.read(b, off, len);
        if (_started == false && count >= 0) _started = true;
        else if(count == -1) _finished = true;
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
        if(_createIntermediateFile == true) {
            renameToIntermediateOrOriginal(true);
        }
        _inputStream = _file.getInputStream();
        if(_logger.isDebugEnabled()) _logger.debug("File stream opened {}", _fileName);
    }

    private void renameToIntermediateOrOriginal(boolean intermediate) {
        String intermediateFileName = null;

        if (intermediate)
            intermediateFileName = "__" + Calendar.getInstance().getTimeInMillis() + "_" + _fileName;
        else
            intermediateFileName = this._originalFileName;

        _file.rename(SMBUtil.trimPath(_directory, intermediateFileName));
        SMBUtil.close(_file);
        _file = _smbConnection.getSmbClient().openFileForRead(intermediateFileName,
                _directory, _smbConnection, _lockTheFileWhileReading);

        _fileName = intermediateFileName;
    }
}
