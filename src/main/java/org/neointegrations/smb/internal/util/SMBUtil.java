package org.neointegrations.smb.internal.util;

import org.mule.extension.file.common.api.matcher.FileMatcher;
import org.mule.extension.file.common.api.matcher.NullFilePayloadPredicate;
import org.mule.runtime.api.connection.ConnectionException;
import org.neointegrations.smb.internal.Constant;
import org.neointegrations.smb.api.SMBFileAttributes;
import org.neointegrations.smb.internal.SMBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

public class SMBUtil {

    private static final Logger _logger = LoggerFactory.getLogger(SMBUtil.class);
    private final static DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ofPattern("YYYYMMddhhmmssSSS");
    private final static String TIME_STAMP_DEFAULT_STR = "TS";

    public static String prepareFilePath(String folder, String fileName) {
        String formattedName = fileName.startsWith(Constant.FRONT_SLASH) ?
                fileName.replace(Constant.FRONT_SLASH, Constant.EMPTY) : fileName;
        StringBuilder dest = new StringBuilder();
        dest.append(folder);
        if (folder.endsWith(Constant.FRONT_SLASH)) {
            dest.append(formattedName.trim());
        } else {
            dest.append(Constant.FRONT_SLASH).append(formattedName.trim());
        }

        _logger.debug("File path {} ", dest.toString());
        return dest.toString();
    }

    public static String fileName(final String path) {
        if(path == null) return null;
        String[] tokens = path.split("[\\\\|/]");
        if(tokens.length == 0) return null;
        return (tokens[tokens.length - 1]).trim();
    }
    public static String directory(final String path) {
        if(path == null) return null;
        String[] tokens = path.split("[\\\\|/]");
        if(tokens.length == 0) return null;
        String filename = tokens[tokens.length - 1];
        return path.replace(filename, "").trim();
    }

    public static Predicate<SMBFileAttributes> getPredicate(FileMatcher builder) {
        return (Predicate)(builder != null ? builder.build() : new NullFilePayloadPredicate());
    }

    public static String trimPath(String directory, String fileName) {
        String dir = directory;
        if (dir.endsWith("/") || dir.endsWith("\\")) {
            dir = dir.substring(0, dir.length() - 1);
        }
        return directory + File.separator + fileName;
    }

    public static void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            _logger.warn("An exception occurred while closing {}", closeable, e);
        }
    }

    public static boolean isConnected(final SMBConnection smbConnection, final String folder)
            throws ConnectionException {

        if(smbConnection == null ||
                smbConnection.getDiskShare() == null) {
            throw new RuntimeException("Connection error, " +
                    "unable to find either connection or diskshare instance");
        }
        try {
            // This operation will raise exception if the connection has been severed
            // The operation will return either true or false for success scenario.
            smbConnection.getDiskShare().folderExists(folder);
        } catch(Exception exp) {
            throw new ConnectionException("Connection error, operation will be retried...");
        }

        return true;
    }

    public static String makeIntermediateFileName(LocalDateTime timestamp, String fName) {
        if(fName == null) return fName;
        String tsStr = timestamp(timestamp);
        return "__" + tsStr + "_" + fName;
    }
    public static String makeIntermediateFileName(String timestamp, String fName) {
        if(fName == null) return fName;
        String tsStr = TIME_STAMP_DEFAULT_STR;
        if(timestamp != null) {
            tsStr = timestamp;
        }
        return "__" + tsStr + "_" + fName;
    }
    public static String timestamp(LocalDateTime timestamp){
        if(timestamp != null) {
            return timestamp.format(TS_FORMATTER);
        } else {
            return TIME_STAMP_DEFAULT_STR;
        }
    }

}
