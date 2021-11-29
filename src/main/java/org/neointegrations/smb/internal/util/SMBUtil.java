package org.neointegrations.smb.internal.util;

import org.mule.extension.file.common.api.matcher.FileMatcher;
import org.mule.extension.file.common.api.matcher.NullFilePayloadPredicate;
import org.neointegrations.smb.internal.Constant;
import org.neointegrations.smb.internal.stream.SMBFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.function.Predicate;

public class SMBUtil {

    private static final Logger _logger = LoggerFactory.getLogger(SMBUtil.class);

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

}
