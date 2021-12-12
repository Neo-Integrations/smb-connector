package org.neointegrations.smb.internal;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.share.Directory;
import com.hierynomus.smbj.share.File;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.core.api.util.IOUtils;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Path;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.neointegrations.smb.internal.util.SMBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.mule.runtime.extension.api.annotation.param.Connection;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.DIRECTORY;
import static org.mule.runtime.extension.api.annotation.param.display.Placement.ADVANCED_TAB;

public class SMBWriteOperations {
    private static final Logger _logger = LoggerFactory.getLogger(SMBWriteOperations.class);


    @Summary("Create a new file in the SMB server using the input content")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Write File")
    public boolean write(@Config final SMBConfiguration smbConfig,
                             @Connection SMBConnection smbConnection,
                             @Optional(defaultValue = "#[payload]") InputStream sourceStream,
                             @Optional(defaultValue = "#[attributes.fileName]") String targetFileName,
                             @Optional(defaultValue = "#['.' ++ (uuid() replace('-') with('_'))]")  String intermediateFileName,
                             @Optional(defaultValue = "false")  boolean createIntermediateFile,
                             @Path(type = DIRECTORY, location = EXTERNAL)
                                @Optional(defaultValue = "/home/share") String targetFolder,
                             @Optional(defaultValue = "true")
                                @Placement( tab = ADVANCED_TAB) boolean overwriteFile,
                             @Optional(defaultValue = "false")
                                @Placement( tab = ADVANCED_TAB) boolean lockFileWhileWriting,
                             @Optional(defaultValue = "true")
                                @Placement( tab = ADVANCED_TAB) boolean createParentDirectory,
                             @Optional(defaultValue = "false")
                                @Placement( tab = ADVANCED_TAB) boolean appendToTargetFile)
            throws ConnectionException {

        _logger.info("Saving file {}", targetFileName);
        boolean fileCreate = false;

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            // smbConnection.getProvider().reconnect();
            throw new ConnectionException("Connection error, operation will be retried...");
        }
        OutputStream outStream = null;
        File fileToWrite = null;
        try {

            if(createParentDirectory) {
                if(!smbConnection.getDiskShare().folderExists(targetFolder)) {
                    smbConnection.getDiskShare().mkdir(targetFolder);
                }
            }

            fileToWrite = smbConnection.getSmbClient().openFileForWrite(
                    (createIntermediateFile == true? intermediateFileName : targetFileName),
                    targetFolder, smbConnection,
                    overwriteFile,  appendToTargetFile,  lockFileWhileWriting);

            outStream = fileToWrite.getOutputStream(appendToTargetFile);
            IOUtils.copyLarge(sourceStream, outStream);

            outStream.flush();
            outStream.close();
            outStream = null;

            if(createIntermediateFile) {
                String targetPath = SMBUtil.prepareFilePath(targetFolder, targetFileName);
                fileToWrite.rename(targetPath);
                fileToWrite.close();
                fileToWrite = null;
            }

            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while writing the file", exp);
            throw new RuntimeException(exp);
        } finally {
                try{ if (outStream != null) outStream.flush(); }catch(Exception ignored){}
                try{ if (outStream != null) outStream.close(); }catch(Exception ignored){}
                try{ if (fileToWrite != null) fileToWrite.close(); }catch(Exception ignored){}
        }
    }

    @Summary("Deleting a file from the SMB server")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Delete File")
    public boolean rmFile(@Config final SMBConfiguration smbConfig,
                         @Connection SMBConnection smbConnection,
                         @Optional(defaultValue = "#[attributes.fileName]") String targetFileName,
                          @Optional(defaultValue = "true") @Placement( tab = ADVANCED_TAB) boolean ignoreErrorWhenFileNotPresent,
                         @Path(type = DIRECTORY, location = EXTERNAL)
                              @Optional(defaultValue = "/home/share") String targetFolder) throws ConnectionException {

        String fileName = SMBUtil.prepareFilePath(targetFolder, targetFileName);

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            //smbConnection.getProvider().reconnect();
        }
        try {
            if(smbConnection.getDiskShare().fileExists(fileName)) {
                smbConnection.getDiskShare().rm(fileName);
            } else if(!ignoreErrorWhenFileNotPresent) {
                throw new IllegalStateException("File does not exists");
            }
            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while deleting the file {}", fileName, exp);
            throw new RuntimeException(exp);
        }

    }

    @Summary("Deleting a folder from the SMB server. If a folder has files, make sure to delete them first first, before deleting the folder. In case for force deleting everything, use the recursive=true")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Remove Directory")
    public boolean rmDir(@Config final SMBConfiguration smbConfig,
                              @Connection SMBConnection smbConnection,
                              @Path(type = DIRECTORY, location = EXTERNAL)
                                @Optional(defaultValue = "/home/share") String targetFolder,
                              @Optional(defaultValue = "true")
                                @Placement( tab = ADVANCED_TAB) boolean ignoreErrorWhenDirNotPresent,
                              @Optional(defaultValue = "true")
                                @Placement( tab = ADVANCED_TAB) boolean recursive) throws ConnectionException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
        }
        try {
            if(smbConnection.getDiskShare().folderExists(targetFolder)) {
                smbConnection.getDiskShare().rmdir(targetFolder, recursive);
            } else if(!ignoreErrorWhenDirNotPresent){
                throw new IllegalStateException("Folder ["+ targetFolder+"] does not exists");
            }
            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while deleting the directory {}", targetFolder, exp);
            throw new RuntimeException(exp);
        }
    }

    @Summary("Create a new directory if not already exists")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Create Directory")
    public boolean mkDir(@Config final SMBConfiguration smbConfig,
                                   @Connection SMBConnection smbConnection,
                                   @Path(type = DIRECTORY, location = EXTERNAL)
                                        @Optional(defaultValue = "/home/share") String targetFolder,
                                   @Optional(defaultValue = "true")
                                        @Placement( tab = ADVANCED_TAB) boolean ignoreErrorOnExists) throws ConnectionException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            //smbConnection.getProvider().reconnect();
        }
        try {
            if(!smbConnection.getDiskShare().folderExists(targetFolder)) {
                smbConnection.getDiskShare().mkdir(targetFolder);
            } else if(!ignoreErrorOnExists) {
                throw new IllegalStateException("Folder ["+ targetFolder+"] already exists");
            }
            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while deleting the directory {}", targetFolder, exp);
            throw new RuntimeException(exp);
        }
    }

    @Summary("Rename directory/file if already exists")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Rename a File / Directory")
    public boolean rename(@Config final SMBConfiguration smbConfig,
                         @Connection SMBConnection smbConnection,
                         @Path(type = DIRECTORY, location = EXTERNAL) @Optional(defaultValue = "/home/share") String sourceFolder,
                         @Optional(defaultValue = "#[null]") String sourceFileName,
                         @Optional(defaultValue = "true") @Placement( tab = ADVANCED_TAB) boolean createParentDirectory,
                         @Path(type = DIRECTORY, location = EXTERNAL) @Optional(defaultValue = "/home/share") String targetFolder,
                         @Optional(defaultValue = "#[null]") String targetFileName,
                         @Optional(defaultValue = "true") @Placement( tab = ADVANCED_TAB) boolean ignoreErrorOnExists
    ) throws ConnectionException {

        boolean isFileRename = false;
        if(sourceFolder == null || targetFolder == null) {
            throw new IllegalArgumentException("Both sourceFolder and targetFolder must be provider");
        }

        if(sourceFileName != null || targetFileName != null) {
            isFileRename = true;
            if(sourceFileName == null || targetFileName == null)
                throw new IllegalArgumentException("Both sourceFileName and targetFileName requires when renaming a file");
        }

        String sourcePath = (sourceFileName != null)? SMBUtil.trimPath(sourceFolder, sourceFileName): sourceFolder;
        String targetPath = (targetFileName != null)? SMBUtil.trimPath(targetFolder, targetFileName): targetFolder;

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
            //smbConnection.getProvider().reconnect();
        }
        try {

            if(createParentDirectory) {
                if(!smbConnection.getDiskShare().folderExists(targetFolder)) {
                    smbConnection.getDiskShare().mkdir(targetFolder);
                }
            }

            if(!isFileRename) {
                if(!smbConnection.getDiskShare().folderExists(targetPath)) {
                    Set<AccessMask> accessMasks = new HashSet();
                    accessMasks.add(AccessMask.MAXIMUM_ALLOWED);
                    try (Directory dir = smbConnection.getDiskShare().openDirectory(sourcePath, accessMasks,
                            null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN,  null))
                    {
                        dir.rename(targetFolder);
                    }
                } else if(!ignoreErrorOnExists) {
                    throw new IllegalStateException("Folder ["+ targetPath+"] already exists");
                }
            } else {
                if(!smbConnection.getDiskShare().fileExists(targetPath)) {
                    try (File sourceFile = smbConnection.getSmbClient().openFileForRead(
                            sourceFileName, sourceFolder, smbConnection, false))
                    {
                        sourceFile.rename(targetPath);

                    }
                } else if(!ignoreErrorOnExists) {
                    throw new IllegalStateException("File ["+ targetPath+"] already exists");
                }
            }

            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while renaming the file / directory {}", targetPath, exp);
            throw new RuntimeException(exp);
        }
    }

    @Summary("Copy a file from one location to another in the remote SB server. It is like the unit command `mv`")
    @MediaType( value = "*/*", strict = false )
    @DisplayName("Remote Copy File")
    public boolean remoteCopy(@Config final SMBConfiguration smbConfig,
                         @Connection SMBConnection smbConnection,
                         @Path(type = DIRECTORY,
                                 location = EXTERNAL) @Optional(defaultValue = "/home/share") String sourceFolder,
                         @Optional(defaultValue = "source.txt") String sourceFileName,
                         @Optional(defaultValue = "true")
                                  @Placement( tab = ADVANCED_TAB) boolean deleteSourceFileAfterRead,
                         @Optional(defaultValue = "false")
                                  @Placement(tab = ADVANCED_TAB) boolean lockFileWhileReading,
                         @Path(type = DIRECTORY,location = EXTERNAL)
                                  @Optional(defaultValue = "/home/share") String targetFolder,
                         @Optional(defaultValue = "target.txt") String targetFileName,
                         @Optional(defaultValue = "true") @Placement(
                                 tab = ADVANCED_TAB) boolean overwriteTargetFile,
                         @Optional(defaultValue = "false") @Placement(
                                 tab = ADVANCED_TAB) boolean lockFileWhileWriting,
                         @Optional(defaultValue = "false") @Placement(
                                 tab = ADVANCED_TAB) boolean appendToTheTargetFile) throws ConnectionException {

        if(smbConnection.getDiskShare() != null &&
                !smbConnection.getDiskShare().isConnected()) {
            throw new ConnectionException("Connection error, operation will be retried...");
           // smbConnection.getProvider().reconnect();
        }
        File sourceFile = null;
        File targetFile = null;
        try {

            sourceFile = smbConnection.getSmbClient().openFileForRead(sourceFileName,
                    sourceFolder, smbConnection, lockFileWhileReading);

            if(deleteSourceFileAfterRead) {
                sourceFile.deleteOnClose();
            }

            targetFile = smbConnection.getSmbClient().openFileForWrite(targetFileName, targetFolder,
                    smbConnection, overwriteTargetFile, appendToTheTargetFile, lockFileWhileWriting);

            sourceFile.remoteCopyTo(targetFile);
            sourceFile.close();
            sourceFile = null;
            targetFile.close();
            targetFile = null;

            return true;
        } catch (Exception exp) {
            _logger.error("Something went wrong while deleting the directory {}", targetFolder, exp);
            throw new RuntimeException(exp);
        } finally {
           if(sourceFile != null) {try{sourceFile.close();} catch(Exception ignored){}}
           if(targetFile != null) { try{targetFile.close();} catch(Exception ignored){}}
        }
    }

}
