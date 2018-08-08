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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.SeekUnsupportedException;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.retry.CountingRetry;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 *
 * The internal bookkeeping works as follows:
 *
 * 1. {@link #updateStream()} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #getPos()}.
 * 2. {@link #getPos()} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream()} is called.
 * 3. {@link #updateStream()} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class FileInStreamV4 extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStreamV4.class);
  private static final int MAX_WORKERS_TO_RETRY =
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_READ_RETRY);

  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;
  private final Input input;
  /* Underlying stream and associated bookkeeping. */

  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;
  private String mTmpBlockPath;

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  protected FileInStreamV4(URIStatus status, InStreamOptions options, FileSystemContext context, String tmpBlockPath) {
    super(status,options,context);
    this.mTmpBlockPath = tmpBlockPath;

    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();
    if (mStatus.getBlockIds().size() != 0) {
      Preconditions.checkState(mStatus.getBlockIds().size() == 1,
          "the file:%s must be consisted of one block", mStatus.getPath());
    } else {
      LOG.warn("file:{} is empty.It should own one block", mStatus.getPath());
    }
    try {
      updateStream();
      if (mBlockInStream instanceof Input) {
        Preconditions.checkState(mBlockInStream instanceof Input,
            "block stream in file:%s is:%s should be local mode ", mStatus.getPath(),
            mBlockInStream != null ? mBlockInStream.getClass() : "null stream"
        );
        input = (Input) mBlockInStream;
      } else {
        throw new SeekUnsupportedException(
            "file stream v2 must get a random block stream.The class of block stream returned" +
                "from block store is:" + (mBlockInStream != null ? mBlockInStream.getClass()
                : "null stream") + " file:" + mStatus.getPath());
      }

    } catch (IOException e) {
      throw new RuntimeException(
          "the fileInStream of path:" + status.getPath() + " failed to update stream", e);
    }

  }

  /**
   * Creates a new file input stream.
   *
   * @param status  the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link FileInStreamV4} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options,
                                    FileSystemContext context, String tmpBlockPath) {
    if (status.getLength() == -1) {
      throw new UnsupportedOperationException("FileInStreamV4 not support unknown size file");
    }
    return new FileInStreamV4(status, options, context, tmpBlockPath);
  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (getPos() == mLength) { // at end of file
      return -1;
    }
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    do {
      try {
        return mBlockInStream.read();
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        handleRetryableException(mBlockInStream, e);
        mBlockInStream = null;
      }
    } while (retry.attempt());
    throw lastException;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }
    if (getPos() == mLength) { // at end of file
      return -1;
    }
    return mBlockInStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    return mBlockInStream.skip(n);
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream(mBlockInStream);
  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mBlockInStream.remaining();
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mLength) {
      return -1;
    }
    return mBlockInStream.positionedRead(pos, b, off, len);
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mBlockInStream.getPos();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (getPos() == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);
    mBlockInStream.seek(pos);
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }
    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    long blockId = mStatus.getBlockIds().get(0);
    // Create stream
    mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      // Get relevant information from the stream.
      stream.close();
    }
  }

  private void handleRetryableException(BlockInStream stream, IOException e) {
    WorkerNetAddress workerAddress = stream.getAddress();
    LOG.warn("Failed to read block {} from worker {}, will retry: {}",
        stream.getId(), workerAddress, e.getMessage());
    try {
      stream.close();
    } catch (Exception ex) {
      // Do not throw doing a best effort close
      LOG.warn("Failed to close input stream for block {}: {}", stream.getId(), ex.getMessage());
    }

    mFailedWorkers.put(workerAddress, System.currentTimeMillis());
  }
  @Override
  public int readByte() throws IOException {
    return input.readByte();
  }

  @Override
  public boolean readBool() throws IOException {
    return input.readBool();
  }

  @Override
  public int readShort() throws IOException {
    return input.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return input.readInt();
  }

  @Override
  public float readFloat() throws IOException {
    return input.readFloat();
  }

  @Override
  public long readLong() throws IOException {
    return input.readLong();
  }

  @Override
  public double readDouble() throws IOException {
    return input.readDouble();
  }

  @Override
  public String readString() throws IOException {
    return input.readString();
  }


  @Override
  public int readByte(int pos) throws IOException {
    return input.readByte(pos);
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    return input.readBool(pos);
  }

  @Override
  public int readShort(int pos) throws IOException {
    return input.readShort(pos);
  }

  @Override
  public int readInt(int pos) throws IOException {
    return input.readInt(pos);
  }

  @Override
  public float readFloat(int pos) throws IOException {
    return input.readFloat(pos);
  }

  @Override
  public long readLong(int pos) throws IOException {
    return input.readLong(pos);

  }

  @Override
  public double readDouble(int pos) throws IOException {
    return input.readDouble(pos);
  }

  @Override
  public String readString(int pos) throws IOException {
    return input.readString(pos);
  }

  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    input.readBytes(bytes, pos);
  }
}
