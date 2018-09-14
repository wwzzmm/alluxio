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

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.block.ByteUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.Input;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Provides an {@link InputStream} implementation that is based on {@link PacketReader}s to
 * stream data packet by packet.
 *
 * file size larger than 2G can use this stream to read
 */
@NotThreadSafe
public class BlockInStreamV2MultiMap extends BlockInStream implements Input {
    private static final Logger LOG = LoggerFactory.getLogger(BlockInStreamV2MultiMap.class);
    private static final long READ_TIMEOUT_MS =
            Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
    private final WorkerNetAddress mAddress;
    private final BlockInStreamSource mInStreamSource;
    /**
     * The id of the block or UFS file to which this instream provides access.
     */
    private final long mId;
    /**
     * The size in bytes of the block.
     */
    private final long mLength;
    private final byte[] mSingleByte = new byte[1];
    private final FileSystemContext mContext;
    private final long mBlockId;
    private final String mPath;
    private RandomAccessFile mLocalFile;
    private FileChannel mLocalFileChannel;
    /**
     * The current packet.
     */
    private boolean mClosed = false;
    private ByteBuffer[] mBuffer;
    private Closer mCloser = Closer.create();

    /**
     * current read postion
     */
    private long position = 0;
    /**
     * current position buffer index
     */
    private int index = 0;


    /**
     * 不能接近2G，否则结尾不足时扩展会超过2G
     */
    private final long mmapSize = Configuration.getLong(PropertyKey.USER_FILE_MMAP_BYTES);

    /**
     * Creates an instance of {@link BlockInStreamV2MultiMap}.
     *
     * @param address     the address of the netty data server
     * @param blockSource the source location of the block
     * @param id          the ID (either block ID or UFS file ID)
     * @param length      the length
     */
    protected BlockInStreamV2MultiMap(WorkerNetAddress address,
                                      BlockInStreamSource blockSource, long id, long length, FileSystemContext context,
                                      long blockId, InStreamOptions options) throws IOException {
        super(null, address, null, id, length);
        mAddress = address;
        mInStreamSource = blockSource;
        mId = id;
        mLength = length;
        mContext = context;
        mBlockId = blockId;
        mPath = getPath(address, context, options);
        mBuffer = map(mPath);
    }

    /**
     * Creates a {@link BlockInStreamV2MultiMap}.
     * <p>
     * One of several read behaviors:
     * <p>
     * 1. Domain socket - if the data source is the local worker and the local worker has a domain
     * socket server
     * 2. Short-Circuit - if the data source is the local worker
     * 3. Local Loopback Read - if the data source is the local worker and short circuit is disabled
     * 4. Read from remote worker - if the data source is a remote worker
     * 5. UFS Read from worker - if the data source is UFS, read from the UFS policy's designated
     * worker (ufs -> local or remote worker -> client)
     *
     * @param context    the file system context
     * @param info       the block info
     * @param dataSource the Alluxio worker which should read the data
     * @param dataSource the source location of the block
     * @param options    the instream options
     * @return the {@link BlockInStreamV2MultiMap} object
     */
    public static BlockInStreamV2MultiMap create(FileSystemContext context, BlockInfo info,
                                                 WorkerNetAddress dataSource, InStreamOptions options)
            throws IOException {
        URIStatus status = options.getStatus();
        OpenFileOptions readOptions = options.getOptions();

        boolean promote = readOptions.getReadType().isPromote();

        long blockId = info.getBlockId();
        long blockSize = info.getLength();

        // Construct the partial read request
        Protocol.ReadRequest.Builder builder =
                Protocol.ReadRequest.newBuilder().setBlockId(blockId).setPromote(promote);
        // Add UFS fallback options
        builder.setOpenUfsBlockOptions(options.getOpenUfsBlockOptions(blockId));
        // Short circuit
        LOG.debug("Creating short circuit input stream for block {} @ {}", blockId, dataSource);
        try {
            return createLocalBlockInStream(context, dataSource, blockId, blockSize, options);
        } catch (NotFoundException e) {
            // Failed to do short circuit read because the block is not available in Alluxio.
            // We will try to read via netty. So this exception is ignored.
            LOG.warn("Failed to create short circuit input stream for block {} @ {}. Falling back to "
                    + "network transfer", blockId, dataSource);
            throw new RuntimeException("block:" + blockId, e);
        }
    }

    /**
     * Creates a {@link BlockInStreamV2MultiMap} to read from a local file.
     *
     * @param context the file system context
     * @param address the network address of the netty data server to read from
     * @param blockId the block ID
     * @param length  the block length
     * @param options the in stream options
     * @return the {@link BlockInStreamV2MultiMap} created
     */
    private static BlockInStreamV2MultiMap createLocalBlockInStream(FileSystemContext context,
                                                                    WorkerNetAddress address, long blockId, long length, InStreamOptions options)
            throws IOException {
        long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);
        return new BlockInStreamV2MultiMap(
                address, BlockInStreamSource.LOCAL, blockId, length, context, blockId, options);
    }

    public static String toString(byte[] b) {
        if (b.length == 0) {
            return null;
        }
        try {
            return new String(b, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getPath(WorkerNetAddress address, FileSystemContext context,
                             InStreamOptions options)
            throws IOException {
        Channel mChannel = context.acquireNettyChannel(address);
        Protocol.LocalBlockOpenRequest request =
                Protocol.LocalBlockOpenRequest.newBuilder().setBlockId(mBlockId)
                        .setPromote(options.getOptions().getReadType().isPromote()).build();
        try {
            ProtoMessage message = NettyRPC
                    .call(NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
                            new ProtoMessage(request));
            Preconditions.checkState(message.isLocalBlockOpenResponse());
            return message.asLocalBlockOpenResponse().getPath();

        } catch (Exception e) {
            context.releaseNettyChannel(address, mChannel);
            throw e;
        }
    }

    private ByteBuffer[] map(String path) throws IOException {
        String mFilePath = Preconditions.checkNotNull(path, "path");
        mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "r"));
        mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
        long length = mLocalFile.length();
        int offset = 0;
        ByteBuffer[] buffers = new ByteBuffer[1];
        if (length > 0) {
            buffers = new ByteBuffer[(int) Math.ceil(length * 1.0 / mmapSize * 1.0)];
            for (int i = 0; i < buffers.length - 1; i++) {
                buffers[i] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset + i * mmapSize, mmapSize);
            }
            buffers[buffers.length - 1] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset + (buffers.length - 1) * mmapSize, (length - (buffers.length - 1) * mmapSize));
        } else {
            buffers[0] = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
        }
        return buffers;

    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public int read() throws IOException {
        int bytesRead = read(mSingleByte);
        if (bytesRead == -1) {
            return -1;
        }
        Preconditions.checkState(bytesRead == 1);
        return BufferUtils.byteToInt(mSingleByte[0]);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkIfClosed();
        Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
        Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
                PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
        if (len == 0) {
            return 0;
        }
        if (len > (mLength - position)) {
            throw new IllegalArgumentException("file remain " + (mLength - position) + "is less than len " + len);
        }
        if (off + len > b.length) {
            throw new IllegalArgumentException("should not read offset+length " + (off + len) + "larger than bytes.length" + b.length);
        }
        try {
            mBuffer[index].get(b, off, len);
        } catch (Exception ignore) {
            getConnectBytes(b, index, off, len);
            index++;
        }
        position += len;
        return len;
    }

    @Override
    public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (pos < 0 || pos >= mLength) {
            return -1;
        }
        long tPos = getPos();
        seek(pos);
        int r = read(b, off, Math.min(b.length - off, len));
        seek(tPos);
        return r;
    }

    @Override
    public long remaining() {
        return mLength - position;
    }

    @Override
    public void seek(long pos) throws IOException {
        long dstPos = pos;
        checkIfClosed();
        Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
        Preconditions
                .checkArgument(pos <= mLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_REGION.toString(),
                        mId);
        if (pos == getPos()) {
            return;
        }
        int seekIndex = (int) (pos / mmapSize);
        if (pos > mLength || pos < 0 || seekIndex >= mBuffer.length) {
            throw new IllegalArgumentException();
        }
        dstPos -= mmapSize * seekIndex;
        mBuffer[seekIndex].position((int) dstPos);
//      将seekIndex前的标记为已读
        if (index < seekIndex) {
            for (int i = 0; i < seekIndex; i++) {
                mBuffer[i].position(mBuffer[i].capacity());
            }
            index = seekIndex;
        }
//        将回退的buffer的position置为0
        else if (index > seekIndex) {
            for (int i = seekIndex + 1; i < mBuffer.length; i++) {
                mBuffer[i].position(0);
            }
            index = seekIndex;
        }
        position = pos;
    }

    @Override
    public long skip(long n) throws IOException {
        checkIfClosed();
        if (n <= 0) {
            return 0;
        }
        if (n > (mLength - position)) {
            throw new IllegalArgumentException();
        }
        int skipIndex = (int) ((position + n) / mmapSize);
        int innerPosition = (int) (position + n);
        innerPosition -= mmapSize * skipIndex;
        mBuffer[skipIndex].position(innerPosition);
//      标记被跳过的buffer全部被读取完
        if (index != skipIndex) {
            for (int i = 0; i < skipIndex; i++) {
                mBuffer[i].position(mBuffer[i].capacity());
            }
            index = skipIndex;
        }
        position += n;
        return n;
    }

    @Override
    public void close() throws IOException {
        mCloser.close();
        for (ByteBuffer buffer : mBuffer) {
            BufferUtils.cleanDirectBuffer(buffer);
        }
        mClosed = true;
    }

    /**
     * Convenience method to ensure the stream is not closed.
     */
    private void checkIfClosed() {
        Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }

    /**
     * @return the address of the data server
     */
    public WorkerNetAddress getAddress() {
        return mAddress;
    }

    /**
     * @return the source of the block location
     */
    public BlockInStreamSource getSource() {
        return mInStreamSource;
    }

    /**
     * @return the block ID
     */
    public long getId() {
        return mId;
    }

    @Override
    public int readByte() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int result;
        try {
            result = mBuffer[index].get() & 0xff;
        } catch (BufferUnderflowException ignore) {
            index++;
            result = mBuffer[index].get() & 0xff;
        }
        position++;
        return result;
    }

    @Override
    public boolean readBool() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        return readByte() == 1;
    }

    @Override
    public int readShort() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int result;
        try {
            result = mBuffer[index].getShort() & 0xffff;
        } catch (BufferUnderflowException ignore) {
            result = ByteUtils.toShort(getConnectBytes(new byte[2], index));
            index = caculateIndex(position,2);
        }
        position += 2;
        return result;
    }

    @Override
    public int readInt() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int result;
        try {
            result = mBuffer[index].getInt();
        } catch (BufferUnderflowException ignore) {
            result = ByteUtils.toInt(getConnectBytes(new byte[4], index));
            index = caculateIndex(position,4);
        }
        position += 4;
        return result;
    }

    @Override
    public float readFloat() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        float result;
        try {
            result = mBuffer[index].getFloat();
        } catch (BufferUnderflowException ignore) {
            result = ByteUtils.toFloat(getConnectBytes(new byte[4], index));
            index = caculateIndex(position,4);
        }
        position += 4;
        return result;
    }

    @Override
    public long readLong() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        long result;
        try {
            result = mBuffer[index].getLong();
        } catch (BufferUnderflowException ignore) {
            result = ByteUtils.toLong(getConnectBytes(new byte[8], index));
            index = caculateIndex(position,8);
        }
        position += 8;
        return result;
    }

    @Override
    public double readDouble() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        double result;
        try {
            result = mBuffer[index].getDouble();
        } catch (BufferUnderflowException ignore) {
            result = ByteUtils.toDouble(getConnectBytes(new byte[8], index));
            index = caculateIndex(position,8);
        }
        position += 8;
        return result;
    }

    @Override
    public String readString() throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int size = readInt();
        byte[] b = new byte[size];
        readBytes(b);
        return ByteUtils.toString(b);
    }

    @Override
    public int readByte(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int posIndex = (int) (pos / mmapSize);
        pos -= mmapSize * posIndex;
        return mBuffer[posIndex].get((int) pos) & 0xff;
    }

    @Override
    public boolean readBool(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        return readByte(pos) == 1;
    }

    @Override
    public int readShort(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int result;
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = 0;
        try {
            tmpPos = (int) (pos - posIndex * mmapSize);
            result = mBuffer[posIndex].getShort(tmpPos) & 0xffff;
        } catch (IndexOutOfBoundsException ignore) {
            result = ByteUtils.toShort(getConnectBytes(new byte[2], posIndex, tmpPos));
        }
        return result;
    }

    @Override
    public int readInt(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int result;
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = 0;
        try {
            tmpPos = (int) (pos - posIndex * mmapSize);
            result = mBuffer[posIndex].getInt(tmpPos);
        } catch (IndexOutOfBoundsException ignore) {
            result = ByteUtils.toInt(getConnectBytes(new byte[4], posIndex, tmpPos));
        }
        return result;
    }

    @Override
    public float readFloat(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        float result;
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = 0;
        try {
            tmpPos = (int) (pos - posIndex * mmapSize);
            result = mBuffer[posIndex].getFloat(tmpPos);
        } catch (IndexOutOfBoundsException ignore) {
            result = ByteUtils.toFloat(getConnectBytes(new byte[4], posIndex, tmpPos));
        }
        return result;
    }

    @Override
    public long readLong(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        long result;
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = 0;
        try {
            tmpPos = (int) (pos - posIndex * mmapSize);
            result = mBuffer[posIndex].getLong(tmpPos);
        } catch (IndexOutOfBoundsException ignore) {
            result = ByteUtils.toLong(getConnectBytes(new byte[8], posIndex, tmpPos));
        }
        return result;
    }

    @Override
    public double readDouble(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        double result;
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = 0;
        try {
            tmpPos = (int) (pos - posIndex * mmapSize);
            result = mBuffer[posIndex].getDouble(tmpPos);
        } catch (IndexOutOfBoundsException ignore) {
            result = ByteUtils.toDouble(getConnectBytes(new byte[8], posIndex, tmpPos));
        }
        return result;
    }

    /**
     * @return the current position of the stream
     */
    protected long getPosition() {
        return position;
    }

    @Override
    public String readString(long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        int size = readInt(pos);
        byte[] bytes = new byte[size];
        readBytes(bytes, pos + 4);
        return toString(bytes);

    }

    @Override
    public void readBytes(byte[] bytes, long pos) throws IOException {
        if (mClosed) {
            Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
        }
        if (bytes.length > (mLength - pos)) {
            throw new IllegalArgumentException("file left size  " + (mLength - pos) + " less than read size:" + bytes.length);
        }
        int posIndex = (int) (pos / mmapSize);
        int tmpPos = (int) (pos - posIndex * mmapSize);
        int length = bytes.length;
        if (length <= (mBuffer[posIndex].capacity() - tmpPos)) {
            for (int i = 0; i < length; i++) {
                bytes[i] = mBuffer[posIndex].get(tmpPos + i);
            }
        } else {
            getConnectBytes(bytes, posIndex, tmpPos);
        }
    }

    @Override
    public ByteBuffer[] getByteBuffers() {
        return mBuffer;
    }

    // TODO: 2018/6/12 wang 如果数组长度大于一段map长度，需要跨多个buffer进行读取了
    private void readBytes(byte[] bytes) throws IOException {
        if (bytes.length > (mLength - position)) {
            throw new IllegalArgumentException("file left size  " + (mLength - position) + " less than read size:" + bytes.length);
        }
        try {
            mBuffer[index].get(bytes);
        } catch (BufferUnderflowException ignore) {
            getConnectBytes(bytes, index);
            index = caculateIndex(position,bytes.length);
        }
        position += bytes.length;
    }


    /**
     * 在分段map段与段的交接处，处理数据拼接
     * 下标需要移动
     *
     * @param bytes  返回的bytes数组
     * @param index  读取map的index下标
     * @param offset bytes的起始index
     * @param length bytes需要填充的字节数
     * @return bytes
     */
    private byte[] getConnectBytes(byte[] bytes, int index, int offset, int length) {
        for (int start = offset; start < offset + length; start++) {
            if (mBuffer[index].hasRemaining()) {
                bytes[start] = mBuffer[index].get();
            } else {
                index++;
                bytes[start] = mBuffer[index].get();
            }
        }
        return bytes;
    }

    /**
     * 在分段map段与段的交接处，处理数据拼接
     * 下标需要移动
     *
     * @param bytes 返回的bytes数组
     * @param index 当前map的index下标
     * @return bytes
     */
    private byte[] getConnectBytes(byte[] bytes, int index) {
        for (int start = 0; start < bytes.length; start++) {
            if (mBuffer[index].hasRemaining()) {
                bytes[start] = mBuffer[index].get();
            } else {
                index++;
                bytes[start] = mBuffer[index].get();
            }
        }
        return bytes;
    }

    /**
     * 在分段map段与段的交接处，处理数据拼接
     * 下标不需要移动
     *
     * @param bytes    返回的bytes数组
     * @param index    读取map的index下标
     * @param position 读取map的position位置
     * @return bytes
     */
    private byte[] getConnectBytes(byte[] bytes, int index, int position) {
        int currentPosition = position;
        for (int start = 0, count = 0; start < bytes.length; start++, count++, position++) {
            if (position != 0 && position % mmapSize == 0) {
                index++;
                currentPosition = 0;
                count = 0;
            }
            bytes[start] = mBuffer[index].get(currentPosition + count);
        }
        return bytes;
    }
    /**
     * 计算出当前的读取下标
     *
     * @param position     当前的全局position
     * @param lengthReaded 已经读取的长度
     * @return
     */
    protected int caculateIndex(long position, int lengthReaded) {
        return (int) ((position + lengthReaded) / mmapSize);
    }
}
