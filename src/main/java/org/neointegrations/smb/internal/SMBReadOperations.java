package org.neointegrations.smb.internal;

import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.protocol.commons.EnumWithValue;
import com.hierynomus.smbj.share.File;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.param.*;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Path;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.neointegrations.smb.api.SMBFileMatcher;
import org.neointegrations.smb.internal.stream.ProgressInputStream;
import org.neointegrations.smb.internal.stream.SMBFileAttributes;
import org.neointegrations.smb.internal.util.SMBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.function.Predicate;


import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.DIRECTORY;
import static org.mule.runtime.extension.api.annotation.param.display.Placement.ADVANCED_TAB;


/**
 * This class is a container for operations, every public method in this class will be taken as an extension operation.
 */
public class SMBReadOperations  {

    private static final Logger _logger = LoggerFactory.getLogger(SMBReadOperations.class);

    @Summary("List all the files from a given directory. It is going to ignore any directory, symlinks, '.' and '..'")
    @MediaType(value = "*/*", strict = false)
    @DisplayName("List a Folder")
    public List<Result<InputStream, SMBFileAttributes>> list(@Config SMBConfiguration smbConfig,
                             @Connection SMBConnection smbConnection,
                             @Optional(defaultValue = "*.*")  String searchPattern,
                             @Optional @DisplayName("File Matching Rules")
                                 @Summary("Matcher to filter the listed files") SMBFileMatcher matcher,
                             @Optional(defaultValue = "false")
                                      @Placement( tab = ADVANCED_TAB) boolean lockTheFileWhileReading,
                             @Optional(defaultValue = "true")
                                      @Placement( tab = ADVANCED_TAB) boolean deleteTheFileAfterRead,
                             @Optional(defaultValue = "1") @Summary("Time Between size Check (in seconds)")
                                 @Placement(tab = ADVANCED_TAB) long timeBetweenSizeCheckInSeconds,
                             @Optional(defaultValue = "true") @Summary("Enable or disable incomplete file check")
                                 @Placement(tab = ADVANCED_TAB) boolean sizeCheckEnabled,
                             @Path(type = DIRECTORY, location = EXTERNAL)
                                     @Optional(defaultValue = "/home/share") String sourceFolder)
            throws ConnectionException, InterruptedException {


        final List<Result<InputStream, SMBFileAttributes>> files = new ArrayList();

        if(_logger.isDebugEnabled()) _logger.debug("Listing a folder...");

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
        }

        final Map<String, Long> nameSizeMap = new HashMap<>();
        if(sizeCheckEnabled) {
            /* Size check of each file is required to determine if a file has
             *   been completely written to the disk before it can be read. Below code will check
             *   the size of each of the selected files between to query separated by timeBetweenSizeCheckInSeconds delay.
             *   The programme will only select those files whose sizes are matched between the queries.
             */
            // First query
            List<FileIdBothDirectoryInformation> firstList =
                    smbConnection.getDiskShare().list(sourceFolder, searchPattern);
            if (_logger.isDebugEnabled()) _logger.debug("First list size: " + firstList.size());

            // Sleep for 2 seconds
            Thread.sleep(timeBetweenSizeCheckInSeconds * 1000);

            // Storing the data in a map
            for (FileIdBothDirectoryInformation file : firstList) {
                nameSizeMap.put(file.getFileName(), file.getEndOfFile());
            }
        }

        // 2nd query
        List<FileIdBothDirectoryInformation> list =
                smbConnection.getDiskShare().list(sourceFolder, searchPattern);

        list.stream().forEach((file) -> {

            // Filter starts
            boolean isHidden = EnumWithValue.EnumUtils.isSet(
                    file.getFileAttributes(), FileAttributes.FILE_ATTRIBUTE_HIDDEN );

            boolean isDirectory = EnumWithValue.EnumUtils.isSet(
                    file.getFileAttributes(), FileAttributes.FILE_ATTRIBUTE_DIRECTORY );

            // Exclude hidden, directory or '.' and '..' from the processing.
            if(isHidden || isDirectory ||
                    (file.getFileName() != null && file.getFileName().startsWith("."))) {
                return;
            }

            if(sizeCheckEnabled) {
                Long fileSize = nameSizeMap.get(file.getFileName());
                if(fileSize != null) {
                    if (fileSize == 0 && file.getEndOfFile() == 0) return;
                    if (file.getEndOfFile() != fileSize) return;
                }
            }

            SMBFileAttributes attr = new SMBFileAttributes(file.getEndOfFile(), true,
                    false, false, sourceFolder,
                    file.getFileName(), file.getChangeTime().toDate());

            Predicate<SMBFileAttributes> match = SMBUtil.getPredicate(matcher);
            if(!match.test(attr)) {
                return;
            }
            // Filters end

            File sourceFile = smbConnection.getSmbClient().openFileForRead(file.getFileName(),
                    sourceFolder, smbConnection,
                    lockTheFileWhileReading);

            files.add(Result.<InputStream, SMBFileAttributes>builder()
                    .output(new ProgressInputStream(smbConnection, sourceFile.getInputStream(), sourceFile, deleteTheFileAfterRead))
                    .attributes(attr)
                    .build());


        });

        return files;
    }

    @Summary("Read a file from the SMB server.")
    @MediaType(value = "*/*", strict = false)
    @DisplayName("Read File")
    public Result<InputStream, SMBFileAttributes> read(@Config SMBConfiguration smbConfig,
                           @Connection SMBConnection smbConnection,
                           @Optional(defaultValue = "/home/share") String sourceFolder,
                           @Optional(defaultValue = "abc.txt") String fileName,
                           @Optional(defaultValue = "false")
                                    @Placement( tab = ADVANCED_TAB) boolean lockFileWhileReading,
                           @Optional(defaultValue = "true")
                                    @Placement( tab = ADVANCED_TAB) boolean deleteFileAfterRead,
                           @Optional(defaultValue = "1") @Summary("Time Between size Check (in seconds)")
                               @Placement(tab = ADVANCED_TAB) long timeBetweenSizeCheckInSeconds,
                           @Optional(defaultValue = "true") @Summary("Enable or disable incomplete file check")
                               @Placement(tab = ADVANCED_TAB) boolean sizeCheckEnabled
                            ) throws ConnectionException, InterruptedException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            // smbConnection.getProvider().reconnect();
        }
        Result<InputStream, SMBFileAttributes> result = null;

        if(fileName == null) {
            throw new IllegalArgumentException("'fileName' must be supplied");
        }

        if(sizeCheckEnabled) {
            /* Size check of each file is required to determine if a file has
             *   been completely written to the disk before it can be read. Below code will check
             *   the size of each of the selected files between to query separated by timeBetweenSizeCheckInSeconds delay.
             *   The programme will only select those files whose sizes are matched between the queries.
             */
            FileAllInformation firstFile = smbConnection.getDiskShare().getFileInformation(SMBUtil.prepareFilePath(sourceFolder, fileName));
            // Sleep for 2 seconds
            Thread.sleep(timeBetweenSizeCheckInSeconds * 1000);
            FileAllInformation nextFile = smbConnection.getDiskShare().getFileInformation(SMBUtil.prepareFilePath(sourceFolder, fileName));

            long firstFileSize = firstFile.getStandardInformation().getEndOfFile();
            long secondFileSize = nextFile.getStandardInformation().getEndOfFile();
            if (firstFileSize == 0 && secondFileSize == 0)
                throw new RuntimeException("Empty file");
            if (firstFileSize != secondFileSize)
                throw new RuntimeException("File is being written by another process. try after sometime...");
        }

        File file = smbConnection.getSmbClient().openFileForRead(fileName, sourceFolder, smbConnection, lockFileWhileReading);

        Date modifiedTime = null;
        if(file.getFileInformation() != null && file.getFileInformation().getBasicInformation() != null &&
                file.getFileInformation().getBasicInformation().getChangeTime() != null &&
                file.getFileInformation().getBasicInformation().getChangeTime().toDate() != null) {
            modifiedTime = file.getFileInformation().getBasicInformation().getChangeTime().toDate();
        }

        InputStream inputStream = file.getInputStream();
        SMBFileAttributes attr = new SMBFileAttributes(0, true,
                false, false,
                SMBUtil.directory(file.getUncPath()), SMBUtil.fileName(file.getUncPath()), modifiedTime);

        result = Result.<InputStream, SMBFileAttributes>builder()
                .output(new ProgressInputStream(smbConnection, inputStream, file, deleteFileAfterRead))
                .attributes(attr)
                .build();

        this._logger.info("Reading the file [{}]", SMBUtil.fileName(file.getUncPath()));

        return result;
    }

    @Summary("Check if the file exists in the SMB server. This can be used before making an actual operations, to ascertain the file exists. For example, before deleting a file, call this operation to check if the file really exists")
    @MediaType(value = "*/*", strict = false)
    @DisplayName("File Exists")
    public boolean fileExists(@Config SMBConfiguration smbConfig,
                              @Connection SMBConnection smbConnection,
                              @Optional(defaultValue = "abc.txt") String fileName,
                              @Path(type = DIRECTORY,
                                     location = EXTERNAL) @Optional(defaultValue = "/home/share") String folder)
            throws ConnectionException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            // smbConnection.getProvider().reconnect();
        }
        String dest = SMBUtil.prepareFilePath(folder, fileName);
        return smbConnection.getDiskShare().fileExists(dest);
    }

    @Summary("Check if the folder exists in the SMB server. This can be used before making an actual operations, to ascertain the folder exists. For example, before deleting a folder, call this operation to check if the folder really exists")
    @MediaType(value = "*/*", strict = false)
    @DisplayName("Folder Exists")
    public boolean folderExists(@Config SMBConfiguration smbConfig,
                              @Connection SMBConnection smbConnection,
                              @Path(type = DIRECTORY,
                                      location = EXTERNAL) @Optional(defaultValue = "/home/share") String folder)
            throws ConnectionException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            // smbConnection.getProvider().reconnect();
        }
        return smbConnection.getDiskShare().folderExists(folder);
    }



}
