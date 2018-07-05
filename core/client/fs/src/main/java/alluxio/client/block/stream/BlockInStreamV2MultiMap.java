///*
// * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
// * (the "License"). You may not use this work except in compliance with the License, which is
// * available at www.apache.org/licenses/LICENSE-2.0
// *
// * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// * either express or implied, as more fully set forth in the License.
// *
// * See the NOTICE file distributed with this work for information regarding copyright ownership.
// */
//
//package alluxio.client.block.stream;
//
//import alluxio.Configuration;
//import alluxio.PropertyKey;
//import alluxio.client.file.FileSystemContext;
//import alluxio.client.file.Input;
//import alluxio.client.file.URIStatus;
//import alluxio.client.file.options.InStreamOptions;
//import alluxio.client.file.options.OpenFileOptions;
//import alluxio.exception.PreconditionMessage;
//import alluxio.exception.status.NotFoundException;
//import alluxio.network.netty.NettyRPC;
//import alluxio.network.netty.NettyRPCContext;
//import alluxio.proto.dataserver.Protocol;
//import alluxio.util.io.BufferUtils;
//import alluxio.util.proto.ProtoMessage;
//import alluxio.wire.BlockInfo;
//import alluxio.wire.WorkerNetAddress;
//import com.google.common.base.Preconditions;
//import com.google.common.io.Closer;
//import io.netty.channel.Channel;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.concurrent.NotThreadSafe;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.RandomAccessFile;
//import java.io.UnsupportedEncodingException;
//import java.nio.BufferUnderflowException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//
///**
// * Provides an {@link InputStream} implementation that is based on {@link PacketReader}s to
// * stream data packet by packet.
// */
//@NotThreadSafe
//public class BlockInStreamV2MultiMap extends BlockInStream implements Input {
//    private static final Logger LOG = LoggerFactory.getLogger(BlockInStreamV2MultiMap.class);
//    private static final long READ_TIMEOUT_MS =
//            Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
//    private final WorkerNetAddress mAddress;
//    private final BlockInStreamSource mInStreamSource;
//    /**
//     * The id of the block or UFS file to which this instream provides access.
//     */
//    private final long mId;
//    /**
//     * The size in bytes of the block.
//     */
//    private final long mLength;
//    private final byte[] mSingleByte = new byte[1];
//    private final FileSystemContext mContext;
//    private final long mBlockId;
//    private final String mPath;
//    private RandomAccessFile mLocalFile;
//    private FileChannel mLocalFileChannel;
//    /**
//     * The current packet.
//     */
//    private boolean mClosed = false;
//    private ByteBuffer[] mBuffer;
//    private Closer mCloser = Closer.create();
//
//    /**
//     * current read postion
//     */
//    private long position = 0;
//    /**
//     * current position buffer
//     */
//    private int index = 0;
//
//
//    /**
//     * 不能接近2G，否则结尾不足时扩展会超过2G
//     */
//    private final long mmapSize = Configuration.getLong(PropertyKey.USER_FILE_MMAP_BYTES);
//
//    /**
//     * Creates an instance of {@link BlockInStreamV2MultiMap}.
//     *
//     * @param address     the address of the netty data server
//     * @param blockSource the source location of the block
//     * @param id          the ID (either block ID or UFS file ID)
//     * @param length      the length
//     */
//    protected BlockInStreamV2MultiMap(WorkerNetAddress address,
//                                      BlockInStreamSource blockSource, long id, long length, FileSystemContext context,
//                                      long blockId, InStreamOptions options) throws IOException {
//        super(null, address, null, id, length);
//        mAddress = address;
//        mInStreamSource = blockSource;
//        mId = id;
//        mLength = length;
//        mContext = context;
//        mBlockId = blockId;
//        mPath = getPath(address, context, options);
//        mBuffer = map(mPath);
//    }
//
//    /**
//     * Creates a {@link BlockInStreamV2MultiMap}.
//     * <p>
//     * One of several read behaviors:
//     * <p>
//     * 1. Domain socket - if the data source is the local worker and the local worker has a domain
//     * socket server
//     * 2. Short-Circuit - if the data source is the local worker
//     * 3. Local Loopback Read - if the data source is the local worker and short circuit is disabled
//     * 4. Read from remote worker - if the data source is a remote worker
//     * 5. UFS Read from worker - if the data source is UFS, read from the UFS policy's designated
//     * worker (ufs -> local or remote worker -> client)
//     *
//     * @param context    the file system context
//     * @param info       the block info
//     * @param dataSource the Alluxio worker which should read the data
//     * @param dataSource the source location of the block
//     * @param options    the instream options
//     * @return the {@link BlockInStreamV2MultiMap} object
//     */
//    public static BlockInStreamV2MultiMap create(FileSystemContext context, BlockInfo info,
//                                                 WorkerNetAddress dataSource, InStreamOptions options)
//            throws IOException {
//        URIStatus status = options.getStatus();
//        OpenFileOptions readOptions = options.getOptions();
//
//        boolean promote = readOptions.getReadType().isPromote();
//
//        long blockId = info.getBlockId();
//        long blockSize = info.getLength();
//
//        // Construct the partial read request
//        Protocol.ReadRequest.Builder builder =
//                Protocol.ReadRequest.newBuilder().setBlockId(blockId).setPromote(promote);
//        // Add UFS fallback options
//        builder.setOpenUfsBlockOptions(options.getOpenUfsBlockOptions(blockId));
//        // Short circuit
//        LOG.debug("Creating short circuit input stream for block {} @ {}", blockId, dataSource);
//        try {
//            return createLocalBlockInStream(context, dataSource, blockId, blockSize, options);
//        } catch (NotFoundException e) {
//            // Failed to do short circuit read because the block is not available in Alluxio.
//            // We will try to read via netty. So this exception is ignored.
//            LOG.warn("Failed to create short circuit input stream for block {} @ {}. Falling back to "
//                    + "network transfer", blockId, dataSource);
//            throw new RuntimeException("block:" + blockId, e);
//        }
//    }
//
//    /**
//     * Creates a {@link BlockInStreamV2MultiMap} to read from a local file.
//     *
//     * @param context the file system context
//     * @param address the network address of the netty data server to read from
//     * @param blockId the block ID
//     * @param length  the block length
//     * @param options the in stream options
//     * @return the {@link BlockInStreamV2MultiMap} created
//     */
//    private static BlockInStreamV2MultiMap createLocalBlockInStream(FileSystemContext context,
//                                                                    WorkerNetAddress address, long blockId, long length, InStreamOptions options)
//            throws IOException {
//        long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);
//        return new BlockInStreamV2MultiMap(
//                address, BlockInStreamSource.LOCAL, blockId, length, context, blockId, options);
//    }
//
//    public static String toString(byte[] b) {
//        if (b.length == 0) {
//            return null;
//        }
//        try {
//            return new String(b, "utf-8");
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    protected String getPath(WorkerNetAddress address, FileSystemContext context,
//                             InStreamOptions options)
//            throws IOException {
//        Channel mChannel = context.acquireNettyChannel(address);
//        Protocol.LocalBlockOpenRequest request =
//                Protocol.LocalBlockOpenRequest.newBuilder().setBlockId(mBlockId)
//                        .setPromote(options.getOptions().getReadType().isPromote()).build();
//        try {
//            ProtoMessage message = NettyRPC
//                    .call(NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
//                            new ProtoMessage(request));
//            Preconditions.checkState(message.isLocalBlockOpenResponse());
//            return message.asLocalBlockOpenResponse().getPath();
//
//        } catch (Exception e) {
//            context.releaseNettyChannel(address, mChannel);
//            throw e;
//        }
//    }
//
//    private ByteBuffer[] map(String path) throws IOException {
//        String mFilePath = Preconditions.checkNotNull(path, "path");
//        mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "r"));
//        mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
//        long length = mLocalFile.length();
//        int offset = 0;
//        ByteBuffer[] buffers = new ByteBuffer[1];
//        if (length > 0) {
//            buffers = new ByteBuffer[(int) Math.ceil(length * 1.0 / mmapSize * 1.0)];
//            for (int i = 0; i < buffers.length - 1; i++) {
//                buffers[i] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset + i * mmapLength, mmapLength);
//            }
//            buffers[buffers.length - 1] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset + (buffers.length - 1) * mmapLength, (length - (buffers.length - 1) * mmapLength));
//        } else {
//            buffers[0] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
//        }
//        return buffers;
////    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mLocalFile.length());
//
//    }
//
//    @Override
//    public long getPos() {
//        return position;
//    }
//
//    @Override
//    public int read() throws IOException {
//        int bytesRead = read(mSingleByte);
//        if (bytesRead == -1) {
//            return -1;
//        }
//        Preconditions.checkState(bytesRead == 1);
//        return BufferUtils.byteToInt(mSingleByte[0]);
//    }
//
//    @Override
//    public int read(byte[] b) throws IOException {
//        return read(b, 0, b.length);
//    }
//
//    @Override
//    public int read(byte[] b, int off, int len) throws IOException {
//        checkIfClosed();
//        Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
//        Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
//                PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
//        if (len == 0) {
//            return 0;
//        }
//        if (len > mmapSize) {
//            throw new IllegalArgumentException("not support illegal offset: " + off + " and length: " + len);
//        }
//        int toRead = Math.min(len, remaining());
//        mBuffer.get(b, off, toRead);
//        return toRead;
//    }
//
//    @Override
//    public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
//        if (len == 0) {
//            return 0;
//        }
//        if (pos < 0 || pos >= mLength) {
//            return -1;
//        }
//        long tPos = getPos();
//        seek(pos);
//        int r = read(b, off, Math.min(b.length - off, len));
//        seek(tPos);
//        return r;
//    }
//
//    @Override
//    public long remaining() {
//        return mLength - position;
//    }
//
//    @Override
//    public void seek(long pos) throws IOException {
//        checkIfClosed();
//        Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
//        Preconditions
//                .checkArgument(pos <= mLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_REGION.toString(),
//                        mId);
//        if (pos == getPos()) {
//            return;
//        }
//        mBuffer.position((int) pos);
//    }
//
//    @Override
//    public long skip(long n) throws IOException {
//        checkIfClosed();
//        if (n <= 0) {
//            return 0;
//        }
//
//        long toSkip = Math.min(remaining(), n);
//        mBuffer.position((int) (getPos() + toSkip));
//        return toSkip;
//    }
//
//    @Override
//    public void close() throws IOException {
//        mCloser.close();
//        BufferUtils.cleanDirectBuffer(mBuffer);
//        mClosed = true;
//    }
//
//    /**
//     * Convenience method to ensure the stream is not closed.
//     */
//    private void checkIfClosed() {
//        Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//    }
//
//    /**
//     * @return the address of the data server
//     */
//    public WorkerNetAddress getAddress() {
//        return mAddress;
//    }
//
//    /**
//     * @return the source of the block location
//     */
//    public BlockInStreamSource getSource() {
//        return mInStreamSource;
//    }
//
//    /**
//     * @return the block ID
//     */
//    public long getId() {
//        return mId;
//    }
//
//    @Override
//    public int readByte() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return read();
//    }
//
//    @Override
//    public boolean readBool() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return readByte() == 1;
//    }
//
//    @Override
//    public int readShort() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getShort() & 0xffff;
//    }
//
//    @Override
//    public int readInt() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getInt();
//    }
//
//    @Override
//    public float readFloat() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getFloat();
//    }
//
//    @Override
//    public long readLong() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getLong();
//    }
//
//    @Override
//    public double readDouble() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getDouble();
//    }
//
//    @Override
//    public String readString() throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        int size = mBuffer.getInt();
//        byte[] bytes = new byte[size];
//        mBuffer.get(bytes);
//        return toString(bytes);
//    }
//
//    @Override
//    public int readByte(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.get(pos) & 0xFF;
//    }
//
//    @Override
//    public boolean readBool(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.get(pos) == 1;
//    }
//
//    @Override
//    public int readShort(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getShort(pos) & 0xffff;
//    }
//
//    @Override
//    public int readInt(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getInt(pos);
//    }
//
//    @Override
//    public float readFloat(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getFloat(pos);
//    }
//
//    @Override
//    public long readLong(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        return mBuffer.getLong(pos);
//    }
//
//    @Override
//    public double readDouble(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        double result;
//        int posIndex = (int) (pos / mmapSize);
//        try {
//            for (int i = 0; i < posIndex; i++) {
//                pos -= mBuffer[i].capacity();
//            }
//            result = mBuffer[posIndex].getDouble(pos);
//        } catch (IndexOutOfBoundsException ignore) {
//            int missingBytes = (8 - (mBuffer[posIndex].limit() - pos));
//            remap(missingBytes, posIndex);
//            result = mBuffer[posIndex].getDouble(pos);
//        }
//        return result;
//    }
//
//    /**
//     * @return the current position of the stream
//     */
//    protected long getPosition() {
//        return position;
//    }
//
//    @Override
//    public String readString(int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        int size = readInt(pos);
//        byte[] bytes = new byte[size];
//        int oldPos = mBuffer[index].position();
//        int oldIndex = index;
//        readBytes(bytes, pos + 4);
//        mBuffer[index].position(oldPos);
//        index = oldIndex;
//        return toString(bytes);
//
//    }
//
//    @Override
//    public void readBytes(byte[] bytes, int pos) throws IOException {
//        if (mClosed) {
//            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
//        }
//        if (bytes.length > mmapSize) {
//            throw new IllegalArgumentException("multi map not support read larger than mmapSize once! size:" + bytes.length);
//        }
//        int oldPosition = (int) position;
//        int oldIndex = index;
//        int currentIndex = (int) (pos / mmapSize);
//        int oldPos = mBuffer[currentIndex].position();
//        for (int i = 0; i < currentIndex; i++) {
//            pos -= mBuffer[i].capacity();
//        }
//        mBuffer[currentIndex].position(pos);
//        index = currentIndex;
//        readBytes(bytes);
//        position = oldPosition;
//        index = oldIndex;
//        mBuffer[currentIndex].position(oldPos);
//    }
//    // TODO: 2018/6/12 wang 如果数组长度大于一段map长度，需要跨多个buffer进行读取了
//    private void readBytes(byte[] bytes) throws IOException {
//        if (bytes.length > mmapSize) {
//            throw new IllegalArgumentException("read " + bytes.length + "! multi map not support read larger than mmapSize once");
//        }
//        try {
//            mBuffer[index].get(bytes);
//        } catch (BufferUnderflowException ignore) {
//            int missingBytes = bytes.length - mBuffer[index].remaining();
//            if (missingBytes != bytes.length) {
//                remap(missingBytes, index);
//                mBuffer[index].get(bytes);
//                index++;
//            } else {
//                isIndexOutOfBounds(missingBytes);
//                index++;
//                mBuffer[index].get(bytes);
//            }
//        }
//        position += bytes.length;
//    }
//
//
//    private ByteBuffer remap(int missingBytes, int bufferIndex) throws IOException {
//        if (bufferIndex >= mBuffer.length - 1) {
//            throw new IllegalArgumentException("mBuffer.length: " + mBuffer.length + "and illegal bufferIndex: " + bufferIndex);
//        }
//        ByteBuffer copyBuffer = mBuffer[bufferIndex];
//        int currentPosition = mBuffer[bufferIndex].position();
//        long currentCapacity = mBuffer[bufferIndex].capacity();
//        ByteBuffer copyNextBuffer = mBuffer[bufferIndex + 1];
//        ByteBuffer finalCopyNextBuffer = copyNextBuffer;
//        int nextCapacity = copyNextBuffer.capacity();
//        mBuffer[bufferIndex + 1] = null;
//        mBuffer[bufferIndex] = null;
//
//        new Thread(() -> {
//            BufferUtils.cleanDirectBuffer(copyBuffer);
//            BufferUtils.cleanDirectBuffer(finalCopyNextBuffer);
//        }).start();
//
//
//        mBuffer[bufferIndex] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY,(bufferIndex + 1) * mmapSize - currentCapacity, currentCapacity + missingBytes);
//        mBuffer[bufferIndex].position(currentPosition);
//        mBuffer[bufferIndex + 1] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY,(bufferIndex + 1) * mmapSize + missingBytes, nextCapacity - missingBytes);
//
//        return mBuffer[bufferIndex];
//
//    }
//    private void isIndexOutOfBounds(int missingBytes) {
//        if (missingBytes > remaining()) {
//            throw new IndexOutOfBoundsException("read " + missingBytes + "! read larger than remaining=" + remaining());
//        }
//    }
//}
