/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.retry.CountingRetry;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of the {@link FileSystem} interface. Developers can extend this class
 * instead of implementing the interface. This implementation reads and writes data through
 * {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
 */
@PublicApi
@ThreadSafe
public class BaseFileSystem implements FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);

    private static int RETRY_TIME = Configuration.getInt(PropertyKey.USER_FILE_LOCAL_RETRY_TIME);
    private static long SLEEP_STEP = Configuration
            .getLong(PropertyKey.USER_FILE_LOCAL_RETRY_SLEEP_STEP);
    protected final FileSystemContext mFileSystemContext;
    protected CuratorFramework client;
    private boolean curatorInited = true;

    /**
     * @param context file system context
     * @return a {@link BaseFileSystem}
     */
    public static BaseFileSystem get(FileSystemContext context) {
        return new BaseFileSystem(context);
    }

    /**
     * Constructs a new base file system.
     *
     * @param context file system context
     */
    protected BaseFileSystem(FileSystemContext context) {
        mFileSystemContext = context;
        initZK();
    }


    private void initZK() {
        if (zkEnable()) {
            try {
                String zookeeperConnectionString = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
                RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
                client = CuratorFrameworkFactory.newClient(
                        zookeeperConnectionString, retryPolicy);
                client.start();
            } catch (Exception e) {
                LOG.error("failet to init zookeeper client,set variable of using lock to false", e);
                curatorInited = false;
            }
        }
    }

    private boolean zkEnable() {
        return Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED) && curatorInited;
    }

    @Override
    public void createDirectory(AlluxioURI path)
            throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        createDirectory(path, CreateDirectoryOptions.defaults());
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
            throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.createDirectory(path, options);
            LOG.debug("Created directory {}, options: {}", path.getPath(), options);
        } catch (AlreadyExistsException e) {
            throw new FileAlreadyExistsException(e.getMessage());
        } catch (InvalidArgumentException e) {
            throw new InvalidPathException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public FileOutStream createFile(AlluxioURI path)
            throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        return createFile(path, CreateFileOptions.defaults());
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
            throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        URIStatus status;
        try {
            masterClient.createFile(path, options);
            // Do not sync before this getStatus, since the UFS file is expected to not exist.
            status = masterClient.getStatus(path,
                    GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
                            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(-1)));
            LOG.debug("Created file {}, options: {}", path.getPath(), options);
        } catch (AlreadyExistsException e) {
            throw new FileAlreadyExistsException(e.getMessage());
        } catch (InvalidArgumentException e) {
            throw new InvalidPathException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
        OutStreamOptions outStreamOptions = options.toOutStreamOptions();
        outStreamOptions.setUfsPath(status.getUfsPath());
        outStreamOptions.setMountId(status.getMountId());
        try {
            return new FileOutStream(path, outStreamOptions, mFileSystemContext);
        } catch (Exception e) {
            delete(path);
            throw e;
        }
    }

    @Override
    public void delete(AlluxioURI path)
            throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
        delete(path, DeleteOptions.defaults().setUnchecked(true));
    }

    @Override
    public void delete(AlluxioURI path, DeleteOptions options)
            throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.delete(path, options);
            LOG.debug("Deleted {}, options: {}", path.getPath(), options);
        } catch (FailedPreconditionException e) {
            // A little sketchy, but this should be the only case that throws FailedPrecondition.
            throw new DirectoryNotEmptyException(e.getMessage());
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public boolean exists(AlluxioURI path)
            throws InvalidPathException, IOException, AlluxioException {
        return exists(path, ExistsOptions.defaults());
    }

    @Override
    public boolean exists(AlluxioURI path, ExistsOptions options)
            throws InvalidPathException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            // TODO(calvin): Make this more efficient
            masterClient.getStatus(path, options.toGetStatusOptions());
            return true;
        } catch (NotFoundException e) {
            return false;
        } catch (InvalidArgumentException e) {
            // The server will throw this when a prefix of the path is a file.
            // TODO(andrew): Change the server so that a prefix being a file means the path does not exist
            return false;
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public void free(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        free(path, FreeOptions.defaults());
    }

    @Override
    public void free(AlluxioURI path, FreeOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.free(path, options);
            LOG.debug("Freed {}, options: {}", path.getPath(), options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public URIStatus getStatus(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        return getStatus(path, GetStatusOptions.defaults());
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            return masterClient.getStatus(path, options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        return listStatus(path, ListStatusOptions.defaults());
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        // TODO(calvin): Fix the exception handling in the master
        try {
            return masterClient.listStatus(path, options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated since version 1.1 and will be removed in version 2.0
     */
    @Deprecated
    @Override
    public void loadMetadata(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        loadMetadata(path, LoadMetadataOptions.defaults());
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated since version 1.1 and will be removed in version 2.0
     */
    @Deprecated
    @Override
    public void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.loadMetadata(path, options);
            LOG.debug("Loaded metadata {}, options: {}", path.getPath(), options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
            throws IOException, AlluxioException {
        mount(alluxioPath, ufsPath, MountOptions.defaults());
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
            throws IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            // TODO(calvin): Make this fail on the master side
            masterClient.mount(alluxioPath, ufsPath, options);
            LOG.info("Mount " + ufsPath.toString() + " to " + alluxioPath.getPath());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            return masterClient.getMountTable();
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public FileInStream openFile(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        return openFile(path, OpenFileOptions.defaults());
    }

    @Override
    public FileInStream openFile(AlluxioURI path, OpenFileOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        URIStatus status = getStatus(path);
        if (status.isFolder()) {
            throw new FileNotFoundException(
                    ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
        }
        InStreamOptions inStreamOptions = options.toInStreamOptions(status);
        int version = options.getVersion();
        if (version == 1) {
            return new FileInStream(status, inStreamOptions, mFileSystemContext);
        } else {
            try {
                if (!isEmptyFile(status)) {
                    localize(path, status, SLEEP_STEP);
                    status = getStatus(path);
                    if (!isLocalWorker(fetchBlockInfo(status))) {
                        if (version == 1024) {
                            return createLocalDiskStream(status, inStreamOptions);
                        }
                        return createNormalStream(status, inStreamOptions);
                    } else {
                        return createStreamV2(status, inStreamOptions, version);
                    }
                } else {
                    return new FileInStreamEmpty(status, inStreamOptions, mFileSystemContext);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst)
            throws FileDoesNotExistException, IOException, AlluxioException {
        rename(src, dst, RenameOptions.defaults());
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            // TODO(calvin): Update this code on the master side.
            masterClient.rename(src, dst, options);
            LOG.debug("Renamed {} to {}, options: {}", src.getPath(), dst.getPath(), options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public void setAttribute(AlluxioURI path)
            throws FileDoesNotExistException, IOException, AlluxioException {
        setAttribute(path, SetAttributeOptions.defaults());
    }

    @Override
    public void setAttribute(AlluxioURI path, SetAttributeOptions options)
            throws FileDoesNotExistException, IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.setAttribute(path, options);
            LOG.debug("Set attributes for {}, options: {}", path.getPath(), options);
        } catch (NotFoundException e) {
            throw new FileDoesNotExistException(e.getMessage());
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    @Override
    public void unmount(AlluxioURI path) throws IOException, AlluxioException {
        unmount(path, UnmountOptions.defaults());
    }

    @Override
    public void unmount(AlluxioURI path, UnmountOptions options)
            throws IOException, AlluxioException {
        FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
        try {
            masterClient.unmount(path);
            LOG.debug("Unmounted {}, options: {}", path.getPath(), options);
        } catch (UnavailableException e) {
            throw e;
        } catch (AlluxioStatusException e) {
            throw e.toAlluxioException();
        } finally {
            mFileSystemContext.releaseMasterClient(masterClient);
        }
    }

    private FileInStream createNormalStream(URIStatus status, InStreamOptions inStreamOptions) {
        LOG.warn(
                "Oops!path:{} localization is failed after retry {} times,and return normal file in stream that can't random read",
                status.getPath(),
                RETRY_TIME);
        return new FileInStream(status, inStreamOptions, mFileSystemContext);
    }

    private FileInStream createStreamV2(URIStatus status, InStreamOptions inStreamOptions,
                                        int version) {
        LOG.info("file:{} is local mode,create random input stream ", status.getPath());
        if (version == 2) {
            return new FileInStreamV2(status, inStreamOptions, mFileSystemContext);
        } else {
            throw new RuntimeException(
                    "Check the stream version:" + version + " that is unsupported now");
        }
    }

    private boolean isEmptyFile(URIStatus status) {
        return status.getLength() == 0;
    }

    private void localize(AlluxioURI path, URIStatus status, long sleepStep)
            throws Exception {
        CountingRetry retry = new CountingRetry(RETRY_TIME);
        boolean firstLoop = true;
        BlockInfo blockInfo = fetchBlockInfo(status);
        while (!isLocalWorker(blockInfo) && retry.attempt()) {
            LOG.info("file:{} is not local mode,read and localization.count :{}", status.getPath(),
                    retry.getAttemptCount());
            if (firstLoop) {
                LOG.info("file:{},data info:{}", status.getPath(), fileInfo(blockInfo));
                firstLoop = false;
            }
            processLocalization(path, status);
            blockInfo = fetchBlockInfo(status);
            if (!isLocalWorker(blockInfo)) {
                Thread.sleep(sleepStep * retry.getAttemptCount());
            } else {
                break;
            }
        }
    }

    private String bindHostname(String path) {
        return AlluxioURI.SEPARATOR + NetworkAddressUtils.getLocalHostName() + path;
    }

    public void processLocalization(AlluxioURI path, URIStatus status) throws Exception {
        if (zkEnable()) {
            InterProcessMutex lock = new InterProcessMutex(client, bindHostname(path.getPath()));
            if (lock.acquire(60, TimeUnit.SECONDS)) {
                try {
                    if (!isLocalWorker(fetchBlockInfo(status))) {
                        forceLocal(status);
                    }
                } finally {
                    lock.release();
                }
            } else {
                LOG.info("file:{} failed to get lock", path.getPath());
                Thread.sleep(3000);
            }
        } else {
            forceLocal(status);
        }
    }

    private void forceLocal(URIStatus status) throws Exception {
        LOG.info("start localization.read file:{} and load data into local worker:{}",
                status.getPath(),
                NetworkAddressUtils.getLocalHostName());
        try (Closer closer = Closer.create()) {
            OpenFileOptions options = OpenFileOptions.defaults()
                    .setReadType(ReadType.CACHE)
                    .setVersion(1)
                    .setCacheLocationPolicy(new SpecificHostPolicy(NetworkAddressUtils.getLocalHostName()));
            FileInStream in = closer
                    .register(
                            new FileInStream(status, options.toInStreamOptions(status), mFileSystemContext));
            byte[] buf = new byte[8 * Constants.MB];
            while (true) {
                if (in.read(buf) == -1) {
                    break;
                }
            }
        } finally {
            LOG.info("finish localization, file:{} worker:{}", status.getPath(),
                    NetworkAddressUtils.getLocalHostName());
        }
    }

    private String fileInfo(BlockInfo info) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("block:").append(info.getBlockId()).append(" ");

        if (info.getLocations().isEmpty()) {
            sb.append(" not exist in alluxio");
        } else {
            for (BlockLocation location : info.getLocations()) {

                sb.append(" worker:[ id:").append(location.getWorkerId())
                        .append(", host:").append(location.getWorkerAddress().getHost()).append("]")
                        .append("\n");
            }
            sb.append("local client host:").append(NetworkAddressUtils.getLocalHostName());
        }
        return sb.toString();
    }


    private FileInStream createLocalDiskStream(URIStatus status, InStreamOptions inStreamOptions)
            throws Exception {
        LOG.warn(
                "path:{} is not localization after retry {} times,and config is client read data to support localization,"
                        +
                        "so must extract data into client local disk ",
                status.getPath(),
                RETRY_TIME);
        return createLocalDiskFileStream(status, inStreamOptions);
    }

    private FileInStream createLocalDiskFileStream(URIStatus status, InStreamOptions inStreamOptions)
            throws Exception {
        String dataPath = Configuration.get(PropertyKey.USER_LOCAL_FILE_IN_STREAM_TMP_PATH);
        FileUtils.forceMkdir(new File(dataPath));
        String fullPath = genTmpPath(dataPath, String.valueOf(status.getBlockIds().get(0)));
        forceLocalDisk(status, fullPath);
        return new FileInStreamV4(status, inStreamOptions, mFileSystemContext, fullPath);
    }

    private static String genTmpPath(String dataPath, String blockID) {

        String name = UUID.randomUUID().toString() + "." + blockID;
        return dataPath + File.separator + name;

    }

    private void forceLocalDisk(URIStatus status, String path) throws Exception {
        if (!isLocalWorker(fetchBlockInfo(status))) {
            LOG.warn("the file:{} is not local mode,to load data into local disk:{}", status.getPath()
                    , path);
            Closer closer = Closer.create();
            RandomAccessFile f = closer.register(new RandomAccessFile(
                    new File(path).getAbsoluteFile(), "rw"));
            FileChannel fc = closer.register(f.getChannel());
            long blockSize = status.getBlockSizeBytes();
            MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, blockSize);
            try {
                OpenFileOptions options = OpenFileOptions.defaults()
                        .setReadType(ReadType.CACHE_PROMOTE)
                        .setLocationPolicy(new SpecificHostPolicy(NetworkAddressUtils.getLocalHostName()));
                FileInStream in = closer
                        .register(
                                new FileInStream(status, options.toInStreamOptions(status), mFileSystemContext));
                byte[] buf = new byte[8 * Constants.MB];
                while (true) {
                    int l = in.read(buf);
                    if (l == -1) {
                        break;
                    }
                    buffer.put(buf, 0, l);
                }
            } catch (Exception e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
                BufferUtils.cleanDirectBuffer(buffer);
            }
        }
    }

    private boolean isLocalWorker(BlockInfo info) throws Exception {
        if (info.getLocations().isEmpty()) {
            return false;
        } else {
            for (BlockLocation location : info.getLocations()) {
                if (location.getWorkerAddress().getHost().equals(NetworkAddressUtils.getLocalHostName())) {
                    return true;
                }
            }
        }
        return false;
    }

    private BlockInfo fetchBlockInfo(URIStatus status) throws Exception {
        Preconditions.checkNotNull(status);
        Preconditions.checkNotNull(status.getBlockIds());
        Preconditions
                .checkState(status.getBlockIds().size() == 1, "the file:%s must be consisted of one block",
                        status.getPath());
        AlluxioBlockStore mBlockStore = AlluxioBlockStore.create(mFileSystemContext);
        return mBlockStore.getInfo(status.getBlockIds().get(0));
    }
}
