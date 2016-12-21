package fdfs_client

import (
    "bytes"
    "encoding/binary"
    "errors"
    "fmt"
    "net"
    "os"
    "time"

    "github.com/laohanlinux/go-logger/logger"
)

type StorageClient struct {
    pool *ConnectionPool
}

func (this *StorageClient) storageUploadByFilename(tc *TrackerClient,
    storeServ *StorageServer, filename string) (*UploadFileResponse, error) {
    fileInfo, err := os.Stat(filename)
    if err != nil {
        return nil, err
    }

    fileSize := fileInfo.Size()
    fileExtName := getFileExt(filename)

    return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
        STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) storageUploadByBuffer(tc *TrackerClient,
    storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
    bufferSize := len(fileBuffer)

    return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
        STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) storageUploadSlaveByFilename(tc *TrackerClient,
    storeServ *StorageServer, filename string, prefixName string, remoteFileId string) (*UploadFileResponse, error) {
    fileInfo, err := os.Stat(filename)
    if err != nil {
        return nil, err
    }

    fileSize := fileInfo.Size()
    fileExtName := getFileExt(filename)

    return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
        STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, remoteFileId, prefixName, fileExtName)
}

func (this *StorageClient) storageUploadSlaveByBuffer(tc *TrackerClient,
    storeServ *StorageServer, fileBuffer []byte, remoteFileId string, fileExtName string) (*UploadFileResponse, error) {
    bufferSize := len(fileBuffer)

    return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
        STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileId, fileExtName)
}

func (this *StorageClient) storageUploadAppenderByFilename(tc *TrackerClient,
    storeServ *StorageServer, filename string) (*UploadFileResponse, error) {
    fileInfo, err := os.Stat(filename)
    if err != nil {
        return nil, err
    }

    fileSize := fileInfo.Size()
    fileExtName := getFileExt(filename)

    return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
        STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (this *StorageClient) storageAppendByBuffer(tc *TrackerClient, storeServ *StorageServer, fileBuffer []byte,
    groupName string, remoteFileName string) error {

    if remoteFileName == "" || groupName == " " {
        return errors.New("Invalid group name or append file name")
    }

    fileSize := int64(len(fileBuffer))
    logger.Info("unknown filesize", fileSize)
    return this.storageDoAppendBuffer(fileSize, fileBuffer, groupName, remoteFileName)
}

func (this *StorageClient) storageAppendByfileName(tc *TrackerClient, storeServ *StorageServer, localFileName string,
    groupName string, remoteFileName string) error {
    if remoteFileName == "" || groupName == " " {
        return errors.New("Invalid group name or append file name")
    }
    fileInfo, err := os.Stat(localFileName)
    if err != nil {
        return err
    }

    fileSize := fileInfo.Size()
    logger.Info("unknown filesize", fileSize)
    return this.storageDoAppendFile(fileSize, localFileName, groupName, remoteFileName)
}

func (this *StorageClient) storageModifyByBuffer(tc *TrackerClient, storeServ *StorageServer, fileBuffer []byte,
    offset int64, groupName string, remoteFileName string) error {
    if remoteFileName == "" || groupName == " " {
        return errors.New("Invalid group name or append file name")
    }

    fileSize := int64(len(fileBuffer))
    logger.Info("unknown filesize", fileSize)
    return this.storageDoModifyBuffer(storeServ, fileSize, fileBuffer, offset, groupName, remoteFileName)
}

func (this *StorageClient) storageModifyByfileName(tc *TrackerClient, storeServ *StorageServer, localFileName string,
    offset int64, groupName string, remoteFileName string) error {
    if remoteFileName == "" || groupName == " " {
        return errors.New("Invalid group name or append file name")
    }
    fileInfo, err := os.Stat(localFileName)
    if err != nil {
        return err
    }

    fileSize := fileInfo.Size()
    logger.Info("unknown filesize", fileSize)
    return this.storageDoModifyFile(fileSize, localFileName, offset, groupName, remoteFileName)
}
func (this *StorageClient) storageUploadAppenderByBuffer(tc *TrackerClient,
    storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
    bufferSize := len(fileBuffer)

    return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
        STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (this *StorageClient) storageUploadFile(tc *TrackerClient,
    storeServ *StorageServer, fileContent interface{}, fileSize int64, uploadType int,
    cmd int8, masterFilename string, prefixName string, fileExtName string) (*UploadFileResponse, error) {

    var (
        conn        net.Conn
        uploadSlave bool
        headerLen   int64 = 15
        reqBuf      []byte
        err         error
    )

    //conn, err = this.pool.Get()
    conn, err = this.GetStorageConn(fmt.Sprintf("%s:%d", storeServ.ipAddr, storeServ.port))
    logger.Debugf("get the storage connection by self, storage addr %s:%d\n", storeServ.ipAddr, storeServ.port)
    defer conn.Close()
    if err != nil {
        return nil, err
    }

    masterFilenameLen := int64(len(masterFilename))
    if len(storeServ.groupName) > 0 && len(masterFilename) > 0 {
        uploadSlave = true
        // #slave_fmt |-master_len(8)-file_size(8)-prefix_name(16)-file_ext_name(6)
        //       #           -master_name(master_filename_len)-|
        headerLen = int64(38) + masterFilenameLen
    }

    th := &trackerHeader{}
    th.pkgLen = headerLen
    th.pkgLen += int64(fileSize)
    th.cmd = cmd
    th.sendHeader(conn)

    if uploadSlave {
        req := &uploadSlaveFileRequest{}
        req.masterFilenameLen = masterFilenameLen
        req.fileSize = int64(fileSize)
        req.prefixName = prefixName
        req.fileExtName = fileExtName
        req.masterFilename = masterFilename
        reqBuf, err = req.marshal()
    } else {
        req := &uploadFileRequest{}
        req.storePathIndex = uint8(storeServ.storePathIndex)
        req.fileSize = int64(fileSize)
        req.fileExtName = fileExtName
        reqBuf, err = req.marshal()
    }
    if err != nil {
        logger.Warn("uploadFileRequest.marshal error :", err.Error())
        return nil, err
    }
    TcpSendData(conn, reqBuf)

    switch uploadType {
    case FDFS_UPLOAD_BY_FILENAME:
        if filename, ok := fileContent.(string); ok {
            err = TcpSendFile(conn, filename)
        }
    case FDFS_DOWNLOAD_TO_BUFFER:
        if fileBuffer, ok := fileContent.([]byte); ok {
            err = TcpSendData(conn, fileBuffer)
        }
    }
    if err != nil {
        logger.Warn(err)
        return nil, err
    }

    th.recvHeader(conn)
    if th.status != 0 {
        return nil, Errno{int(th.status)}
    }
    recvBuff, recvSize, err := TcpRecvResponse(conn, th.pkgLen)
    if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
        errmsg := "[-] Error: Storage response length is not match, "
        errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }
    ur := &UploadFileResponse{}
    err = ur.unmarshal(recvBuff)
    if err != nil {
        errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }

    // add store obj
    ur.StoreServ = *storeServ
    return ur, nil
}

func (this *StorageClient) storageDeleteFile(tc *TrackerClient, storeServ *StorageServer, remoteFilename string) (*DeleteFileResponse, error) {
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )

    //conn, err = this.pool.Get()
    logger.Debugf("get the storage connection by self, storage addr %s:%d\n", storeServ.ipAddr, storeServ.port)
    conn, err = this.GetStorageConn(fmt.Sprintf("%s:%d", storeServ.ipAddr, storeServ.port))
    defer conn.Close()
    if err != nil {
        return nil, err
    }

    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_DELETE_FILE
    fileNameLen := len(remoteFilename)
    th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + fileNameLen)
    th.sendHeader(conn)

    req := &deleteFileRequest{}
    req.groupName = storeServ.groupName
    req.remoteFilename = remoteFilename
    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err)
        return nil, err
    }
    TcpSendData(conn, reqBuf)

    th.recvHeader(conn)
    if th.status != 0 {
        return nil, Errno{int(th.status)}
    }
    logger.Info("pkg_len:", th.pkgLen)
    /*recvBuff, recvSize, err := TcpRecvResponse(conn, th.pkgLen)
    if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
        errmsg := "[-] Error: Storage response length is not match, "
        errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }*/
    dr := &DeleteFileResponse{}
    /*err = dr.unmarshal(recvBuff)
    if err != nil {
        errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }*/
    return dr, nil
}

func (this *StorageClient) storageDownloadToFile(tc *TrackerClient,
    storeServ *StorageServer, localFilename string, offset int64,
    downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
    return this.storageDownloadFile(tc, storeServ, localFilename, offset, downloadSize, FDFS_DOWNLOAD_TO_FILE, remoteFilename)
}

func (this *StorageClient) storageDownloadToBuffer(tc *TrackerClient,
    storeServ *StorageServer, fileBuffer []byte, offset int64,
    downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
    return this.storageDownloadFile(tc, storeServ, fileBuffer, offset, downloadSize, FDFS_DOWNLOAD_TO_BUFFER, remoteFilename)
}

func (this *StorageClient) storageDownloadFile(tc *TrackerClient,
    storeServ *StorageServer, fileContent interface{}, offset int64, downloadSize int64,
    downloadType int, remoteFilename string) (*DownloadFileResponse, error) {

    var (
        conn          net.Conn
        reqBuf        []byte
        localFilename string
        recvBuff      []byte
        recvSize      int64
        err           error
    )

    conn, err = this.pool.Get()
    defer conn.Close()
    if err != nil {
        return nil, err
    }

    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2 + FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename))
    th.sendHeader(conn)

    req := &downloadFileRequest{}
    req.offset = offset
    req.downloadSize = downloadSize
    req.groupName = storeServ.groupName
    req.remoteFilename = remoteFilename
    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("downloadFileRequest.marshal error :", err.Error())
        return nil, err
    }
    TcpSendData(conn, reqBuf)

    th.recvHeader(conn)
    if th.status != 0 {
        return nil, Errno{int(th.status)}
    }

    switch downloadType {
    case FDFS_DOWNLOAD_TO_FILE:
        if localFilename, ok := fileContent.(string); ok {
            recvSize, err = TcpRecvFile(conn, localFilename, th.pkgLen)
        }
    case FDFS_DOWNLOAD_TO_BUFFER:
        if _, ok := fileContent.([]byte); ok {
            recvBuff, recvSize, err = TcpRecvResponse(conn, th.pkgLen)
        }
    }
    if err != nil {
        logger.Warn(err.Error())
        return nil, err
    }
    if recvSize < downloadSize {
        errmsg := "[-] Error: Storage response length is not match, "
        errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }

    dr := &DownloadFileResponse{}
    dr.RemoteFileId = storeServ.groupName + string(os.PathSeparator) + remoteFilename
    if downloadType == FDFS_DOWNLOAD_TO_FILE {
        dr.Content = localFilename
    } else {
        dr.Content = recvBuff
    }
    dr.DownloadSize = recvSize
    return dr, nil
}

func (this *StorageClient) storageTruncateFile(tc *TrackerClient, storeServ *StorageServer,
    appenderFileName string, truncatedFileSize int64) (*DeleteFileResponse, error) {
    //update connection
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )
    logger.Debugf("get the storage connection by self, storage addr %s:%d\n", storeServ.ipAddr, storeServ.port)
    conn, err = this.GetStorageConn(fmt.Sprintf("%s:%d", storeServ.ipAddr, storeServ.port))
    //conn, err = this.pool.Get()
    defer conn.Close()
    if err != nil {
        return nil, err
    }

    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_TRUNCATE_FILE
    appenderFileNameLen := len(appenderFileName)
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2 + appenderFileNameLen)
    th.sendHeader(conn)
    //logger.Info("1111111")

    req := &truncFileRequest{}
    req.appendernameLen = int64(appenderFileNameLen)
    req.truncatedFileSize = truncatedFileSize
    req.appenderFileName = appenderFileName

    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err.Error())
        return nil, err
    }
    TcpSendData(conn, reqBuf)
    th.recvHeader(conn)
    if th.status != 0 {
        return nil, Errno{int(th.status)}
    }

    logger.Info("pkg_len:", th.pkgLen)

    /*recvBuff, recvSize, err := TcpRecvResponse(conn, th.pkgLen)
    if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
        errmsg := "[-] Error: Storage response length is not match, "
        errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }*/

    dr := &DeleteFileResponse{}
    /*err = dr.unmarshal(recvBuff)
    if err != nil {
        errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
        logger.Warn(errmsg)
        return nil, errors.New(errmsg)
    }*/
    logger.Debug("1")
    return dr, nil

}
func (this *StorageClient) storageQueryFileInfo(sServ *StorageServer, groupName string, remoteFileName string) (*FileInfo, error) {
    var (
        conn     net.Conn
        recvBuff []byte
        err      error
    )

    //conn, err = this.pool.Get()
    conn, err = this.GetStorageConn(fmt.Sprintf("%s:%d", sServ.ipAddr, sServ.port))
    defer conn.Close()
    if err != nil {
        return nil, err
    }
    th := &trackerHeader{}
    th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + len(remoteFileName))
    th.cmd = STORAGE_PROTO_CMD_QUERY_FILE_INFO
    th.sendHeader(conn)
    queryBuffer := new(bytes.Buffer)
    // 16 bit groupName
    groupNameBytes := bytes.NewBufferString(groupName).Bytes()
    for i := 0; i < 16; i++ {
        if i >= len(groupNameBytes) {
            queryBuffer.WriteByte(byte(0))
        } else {
            queryBuffer.WriteByte(groupNameBytes[i])
        }
    }
    // remoteFilenameLen bit remoteFilename
    remoteFilenameBytes := bytes.NewBufferString(remoteFileName).Bytes()
    for i := 0; i < len(remoteFilenameBytes); i++ {
        queryBuffer.WriteByte(remoteFilenameBytes[i])
    }
    err = TcpSendData(conn, queryBuffer.Bytes())
    if err != nil {
        return nil, err
    }

    th.recvHeader(conn)
    if th.status != 0 {
        logger.Warn("recvHeader error:", th.status)
        return nil, Errno{int(th.status)}
    }
    var (
        x               int32
        createTimeStamp int32
        crc32           int32
        fileSize        int64
        ipAddr          string
    )
    logger.Info("pkg_len:", th.pkgLen)
    recvBuff, _, err = TcpRecvResponse(conn, th.pkgLen)
    if err != nil {
        logger.Warn("TcpRecvResponse error :", err.Error())
        return nil, err
    }
    buff := bytes.NewBuffer(recvBuff)
    binary.Read(buff, binary.BigEndian, &fileSize)
    //logger.Infof("filesize:%d", fileSize)
    binary.Read(buff, binary.BigEndian, &x)
    binary.Read(buff, binary.BigEndian, &createTimeStamp)
    //logger.Infof("timestamp:%d", createTimeStamp)
    binary.Read(buff, binary.BigEndian, &x)
    binary.Read(buff, binary.BigEndian, &crc32)
    //logger.Infof("crc32:%d", crc32)
    ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
    if err != nil {
        return nil, err
    }
    //logger.Info("ip:" + ipAddr)
    return &FileInfo{
        CreateTimeStamp: createTimeStamp,
        CRC32:           crc32,
        SourceId:        0,
        FileSize:        fileSize,
        SourceIpAddress: ipAddr,
    }, nil

}

func (this *StorageClient) storageDoAppendBuffer(fileSize int64, fileBuffer []byte, groupName string, remoteFileName string) error {
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )

    conn, err = this.pool.Get()
    defer conn.Close()
    if err != nil {
        return err
    }
    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_APPEND_FILE
    appenderFileNameLen := len(remoteFileName)
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2+appenderFileNameLen) + fileSize
    th.sendHeader(conn)
    req := &truncFileRequest{}
    req.appendernameLen = int64(appenderFileNameLen)
    req.truncatedFileSize = fileSize
    req.appenderFileName = remoteFileName

    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err.Error())
        return err
    }
    TcpSendData(conn, reqBuf)
    TcpSendData(conn, fileBuffer)
    th.recvHeader(conn)
    if th.status != 0 {
        return Errno{int(th.status)}
    }

    logger.Info("pkg_len:", th.pkgLen)

    return nil
}

func (this *StorageClient) storageDoAppendFile(fileSize int64, localFileName string,
    groupName string, remoteFileName string) error {
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )

    conn, err = this.pool.Get()
    defer conn.Close()
    if err != nil {
        return err
    }
    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_APPEND_FILE
    appenderFileNameLen := len(remoteFileName)
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2+appenderFileNameLen) + fileSize
    th.sendHeader(conn)
    req := &truncFileRequest{}
    req.appendernameLen = int64(appenderFileNameLen)
    req.truncatedFileSize = fileSize
    req.appenderFileName = remoteFileName

    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err.Error())
        return err
    }
    TcpSendData(conn, reqBuf)
    TcpSendFile(conn, localFileName)
    th.recvHeader(conn)
    if th.status != 0 {
        return Errno{int(th.status)}
    }

    logger.Info("pkg_len:", th.pkgLen)

    return nil
}

func (this *StorageClient) storageDoModifyFile(fileSize int64, localFileName string, offset int64,
    groupName string, remoteFileName string) error {
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )

    conn, err = this.pool.Get()
    defer conn.Close()
    if err != nil {
        return err
    }
    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_MODIFY_FILE
    appenderFileNameLen := len(remoteFileName)
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*3+appenderFileNameLen) + fileSize
    th.sendHeader(conn)
    req := &modifyFileRequst{}
    req.appendernameLen = int64(appenderFileNameLen)
    req.offset = offset
    req.modifiedFileLen = fileSize
    req.appenderFileName = remoteFileName

    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err.Error())
        return err
    }
    TcpSendData(conn, reqBuf)
    TcpSendFile(conn, localFileName)
    th.recvHeader(conn)
    if th.status != 0 {
        return Errno{int(th.status)}
    }

    logger.Info("pkg_len:", th.pkgLen)

    return nil
}

func (this *StorageClient) storageDoModifyBuffer(sServ *StorageServer, fileSize int64, fileBuffer []byte, offset int64,
    groupName string, remoteFileName string) error {
    var (
        conn   net.Conn
        reqBuf []byte
        err    error
    )
    logger.Debugf("get the storage connection by self, storage addr %s:%d\n", sServ.ipAddr, sServ.port)
    conn, err = this.GetStorageConn(fmt.Sprintf("%s:%d", sServ.ipAddr, sServ.port))
    //conn, err = this.pool.Get()
    if err != nil {
        logger.Errorf("GetStorageConn err:%s, ip:%s", err.Error(), sServ.ipAddr)
        return err
    }

    defer conn.Close()

    th := &trackerHeader{}
    th.cmd = STORAGE_PROTO_CMD_MODIFY_FILE
    appenderFileNameLen := len(remoteFileName)
    th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*3+appenderFileNameLen) + fileSize
    th.sendHeader(conn)
    req := &modifyFileRequst{}
    req.appendernameLen = int64(appenderFileNameLen)
    req.offset = offset
    req.modifiedFileLen = fileSize
    req.appenderFileName = remoteFileName

    reqBuf, err = req.marshal()
    if err != nil {
        logger.Warn("deleteFileRequest.marshal error :", err.Error())
        return err
    }
    TcpSendData(conn, reqBuf)
    TcpSendData(conn, fileBuffer)
    th.recvHeader(conn)
    if th.status != 0 {
        return Errno{int(th.status)}
    }

    logger.Info("pkg_len:", th.pkgLen)
    return nil
}

func (this *StorageClient) GetStorageConn(addr string) (net.Conn, error) {
    c, err := net.DialTimeout("tcp", addr, time.Second*1)
    if err != nil {
        return c, err
    }
    c.SetDeadline(time.Now().Add(time.Duration(30) * time.Second))
    return c, err
}

