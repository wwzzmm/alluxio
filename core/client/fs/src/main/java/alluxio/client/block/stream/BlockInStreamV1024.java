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
import alluxio.client.file.Input;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Provides an {@link InputStream} implementation that is based on {@link PacketReader}s to
 * stream data packet by packet.
 */
@NotThreadSafe
public class BlockInStreamV1024 extends BlockInStream implements Input {
  private static final Logger LOG = LoggerFactory.getLogger(BlockInStreamV1024.class);
  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private final byte[] mSingleByte = new byte[1];
  private final String mPath;
  private final long mLength;
  /** The current packet. */
  private boolean mClosed = false;
  private ByteBuffer mBuffer;
  private Closer mCloser = Closer.create();

  /**
   * Creates an instance of {@link BlockInStreamV1024}.
   *
   * @param length the length
   * @param path the path
   */
  protected BlockInStreamV1024(long length, String path) throws IOException {
    super(null, null, null, 0, 0);
    mLength = length;

    mPath = path;
    mBuffer = map(mPath);
  }

  /**
   * Creates a {@link BlockInStreamV1024}.
   *
   * One of several read behaviors:
   *
   * 1. Domain socket - if the data source is the local worker and the local worker has a domain
   * socket server
   * 2. Short-Circuit - if the data source is the local worker
   * 3. Local Loopback Read - if the data source is the local worker and short circuit is disabled
   * 4. Read from remote worker - if the data source is a remote worker
   * 5. UFS Read from worker - if the data source is UFS, read from the UFS policy's designated
   * worker (ufs -> local or remote worker -> client)
   *
   * @param info the block info
   * @param dataSource the Alluxio worker which should read the data
   * @param options the instream options
   * @return the {@link BlockInStreamV1024} object
   */
  public static BlockInStreamV1024 create(BlockInfo info,
                                          WorkerNetAddress dataSource, InStreamOptions options, String path)
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
      return createLocalBlockInStream(blockSize, path);
    } catch (NotFoundException e) {
      // Failed to do short circuit read because the block is not available in Alluxio.
      // We will try to read via netty. So this exception is ignored.
      LOG.warn("Failed to create short circuit input stream for block {} @ {}. Falling back to "
          + "network transfer", blockId, dataSource);
      throw new RuntimeException("block:" + blockId, e);
    }
  }

  /**
   * Creates a {@link BlockInStreamV1024} to read from a local file.
   *
   * @param length the block length
   * @param path the block path
   * @return the {@link BlockInStreamV1024} created
   */
  private static BlockInStreamV1024 createLocalBlockInStream(long length,
                                                             String path)
      throws IOException {
    return new BlockInStreamV1024(length, path);
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

  private ByteBuffer map(String path) throws IOException {
    String mFilePath = Preconditions.checkNotNull(path, "path");
    RandomAccessFile mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "r"));
    FileChannel mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mLocalFile.length());

  }

  @Override
  public long getPos() {
    return mBuffer.position();
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
    int toRead = Math.min(len, mBuffer.remaining());
    mBuffer.get(b, off, toRead);
    return toRead;
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
    return mBuffer.remaining();
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions
        .checkArgument(pos <= mLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_REGION.toString(),
            0);
    if (pos == getPos()) {
      return;
    }
    mBuffer.position((int) pos);
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(remaining(), n);
    mBuffer.position((int) (getPos() + toSkip));
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
    BufferUtils.cleanDirectBuffer(mBuffer);
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
    throw new UnsupportedOperationException();
  }

  /**
   * @return the source of the block location
   */
  public BlockInStreamSource getSource() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the block ID
   */
  public long getId() {
    return 0;
  }

  @Override
  public int readByte() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return read();
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
    return mBuffer.getShort() & 0xffff;
  }

  @Override
  public int readInt() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getInt();
  }

  @Override
  public float readFloat() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getFloat();
  }

  @Override
  public long readLong() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getLong();
  }

  @Override
  public double readDouble() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getDouble();
  }

  @Override
  public String readString() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int size = mBuffer.getInt();
    byte[] bytes = new byte[size];
    mBuffer.get(bytes);
    return toString(bytes);
  }

  @Override
  public int readByte(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.get(pos) & 0xFF;
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.get(pos) == 1;
  }

  @Override
  public int readShort(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getShort(pos) & 0xffff;
  }

  @Override
  public int readInt(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getInt(pos);
  }

  @Override
  public float readFloat(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getFloat(pos);
  }

  @Override
  public long readLong(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getLong(pos);
  }

  @Override
  public double readDouble(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getDouble(pos);
  }

  /**
   * @return the current position of the stream
   */
  protected long getPosition() {
    return mBuffer.position();
  }

  @Override
  public String readString(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int size = mBuffer.getInt(pos);
    byte[] bytes = new byte[size];
    int tPos = mBuffer.position();
    mBuffer.position(pos + 4);
    mBuffer.get(bytes);
    mBuffer.position(tPos);
    return toString(bytes);
  }

  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int length = bytes.length;
    for(int i = 0 ; i < length ; i++){
      bytes[i] = mBuffer.get(pos + 1);
    }
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return mBuffer;
  }
}
