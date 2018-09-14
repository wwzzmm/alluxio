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
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.*;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

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
 * 1. {@link #updateStream(long blockId)} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #mPosition}.
 * 2. {@link #mPosition} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream(long blockId)} is called.
 * 3. {@link #updateStream(long blockId)} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable, Input {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);
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
  /** Whether to cache blocks in this file into Alluxio. */
  private final boolean mShouldCache;
  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;
  /** Current block out stream writing the data into Alluxio. */
  protected OutputStream mCurrentCacheStream;
  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();
  /** The location policy for CACHE type of read into Alluxio. */
  protected final FileWriteLocationPolicy mLocationPolicy;
  /** The outstream options. */
  private final OutStreamOptions mOutStreamOptions;
  /** The blockId used in the block streams. */
  private long mStreamBlockId;
  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;
    mOutStreamOptions = OutStreamOptions.defaults().setWriteTier(Configuration.getInt(PropertyKey.USER_FILE_IN_STREAM_CACHE_TIER));
    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();
    mLocationPolicy =  CommonUtils.createNewClassInstance(
            Configuration.<FileWriteLocationPolicy>getClass(
                    PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[]{}, new Object[]{});
    mPosition = 0;
    mBlockInStream = null;
    mShouldCache = options.getOptions().getReadType().getAlluxioStorageType().isStore();
  }
  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link FileInStream} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options,
                                    FileSystemContext context) {
    if (status.getLength() == -1) {
      throw new UnsupportedOperationException("FileInStream not support unknown size file");
    }
    return new FileInStream(status, options, context);
  }
  /**
   * Updates {@link #mCurrentCacheStream}. When {@code mShouldCache} is true, {@code FileInStream}
   * will create an {@code BlockOutStream} to cache the data read only if
   * <ol>
   * <li>the file is read from under storage, or</li>
   * <li>the file is read from a remote worker and we have an available local worker.</li>
   * </ol>
   * The following preconditions are checked inside:
   * <ol>
   * <li>{@link #mCurrentCacheStream} is either done or null.</li>
   * <li>EOF is reached or {@link #mBlockInStream} must be valid.</li>
   * </ol>
   * After this call, {@link #mCurrentCacheStream} is either null or freshly created.
   * {@link #mCurrentCacheStream} is created only if the block is not cached in a chosen machine
   * and mPos is at the beginning of a block.
   * This function is only called by {@link #updateStream(long blockId)} ()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   * @throws IOException if the next cache stream cannot be created
   */
  private void updateCacheStream(long blockId) throws IOException {
    // We should really only close a cache stream here. This check is to verify this.
    Preconditions.checkState(mCurrentCacheStream == null || cacheStreamRemaining() == 0);
    closeOrCancelCacheStream();
    Preconditions.checkState(mCurrentCacheStream == null);

    if (blockId < 0) {
      // End of file.
      return;
    }
    Preconditions.checkNotNull(mBlockInStream);
    if (!mShouldCache || isReadingFromLocalBlockWorker()) {
      return;
    }

    // If this block is read from a remote worker but we don't have a local worker, don't cache
    if (isReadingFromRemoteBlockWorker() && !mContext.hasLocalWorker()) {
      return;
    }

    // Unlike updateBlockInStream below, we never start a block cache stream if mPos is in the
    // middle of a block.
    if (mPosition % mBlockSize != 0) {
      return;
    }

    try {
      WorkerNetAddress address = mLocationPolicy.getWorkerForNextBlock(
              mBlockStore.getAllWorkers(), getBlockSizeAllocation(mPosition));
      // If we reach here, we need to cache.
      mCurrentCacheStream =
              mBlockStore.getOutStream(blockId, getBlockSize(mPosition), address, mOutStreamOptions);
    } catch (IOException e) {
      handleCacheStreamIOException(e);
    }
  }

  /**
   * Handles IO exceptions thrown in response to the worker cache request. Cache stream is closed
   * or cancelled after logging some messages about the exceptions.
   *
   * @param e the exception to handle
   */
  private void handleCacheStreamIOException(IOException e) {
    if (e.getCause() instanceof BlockAlreadyExistsException) {
      // This can happen if there are two readers trying to cache the same block. The first one
      // created the block (either as temp block or committed block). The second sees this
      // exception.
      LOG.info(
              "The block with ID {} is already stored in the target worker, canceling the cache "
                      + "request.{}", getCurrentBlockId(),e);
    } else {
      LOG.warn("The block with ID {} could not be cached into Alluxio storage.{}",
              getCurrentBlockId(),e);
    }
    closeOrCancelCacheStream();
  }



  /**
   * If we are not in the last block or if the last block is equal to the normal block size,
   * return the normal block size. Otherwise return the block size of the last block.
   *
   * @param pos the position to get the block size for
   * @return the size of the block that covers pos
   */
  protected long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mLength % mBlockSize;
    if (mLength - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }
  /**
   * @param pos the position to check
   * @return the block size in bytes for the given pos, used for worker allocation
   */
  protected long getBlockSizeAllocation(long pos) {
    return getBlockSize(pos);
  }

  /**
   * @return the remaining bytes in the current cache out stream
   */
  private long cacheStreamRemaining() {
    assert mCurrentCacheStream instanceof BoundedStream;
    return ((BoundedStream) mCurrentCacheStream).remaining();
  }

  /**
   * @return the remaining bytes in the current block in stream
   */
  protected long inStreamRemaining() {
    return ((BoundedStream)mBlockInStream).remaining();
  }
  /**
   * Cancels the current cache out stream.
   *
   * @throws IOException if it fails to cancel the cache out stream
   */
  private void cacheStreamCancel() throws IOException {
    assert mCurrentCacheStream instanceof Cancelable;
    ((Cancelable) mCurrentCacheStream).cancel();
  }

  /**
   * @return true if {@code mBlockInStream} is reading from a local block worker
   *    * if {@code mBlockInStream.getSource()==null we should return true to avoid cache}
   */
  private boolean isReadingFromLocalBlockWorker() {
    return mBlockInStream.getSource()==null?true:(mBlockInStream.getSource().equals(BlockInStream.BlockInStreamSource.LOCAL));
  }

  /**
   * @return true if {@code mBlockInStream} is reading from a remote block worker
   * if {@code mBlockInStream.getSource()==null we should return true to avoid cache}
   */
  private boolean isReadingFromRemoteBlockWorker() {
    return mBlockInStream.getSource()==null?true: ((mBlockInStream.getSource().equals(BlockInStream.BlockInStreamSource.REMOTE))
            || (mBlockInStream.getSource().equals(BlockInStream.BlockInStreamSource.UFS)));
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   */
  private void closeOrCancelCacheStream() {
    if (mCurrentCacheStream == null) {
      return;
    }
    try {
      if (cacheStreamRemaining() == 0) {
        mCurrentCacheStream.close();
      } else {
        cacheStreamCancel();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof BlockDoesNotExistException) {
        // This happens if two concurrent readers read trying to cache the same block. One cancelled
        // before the other. Then the other reader will see this exception since we only keep
        // one block per blockId in block worker.
        LOG.info("Block {} does not exist when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof InvalidWorkerStateException) {
        // This happens if two concurrent readers trying to cache the same block and they acquired
        // different file system contexts.
        // instances (each instance has its only session ID).
        LOG.info("Block {} has invalid worker state when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof BlockAlreadyExistsException) {
        // This happens if two concurrent readers trying to cache the same block. One successfully
        // committed. The other reader sees this.
        LOG.info("Block {} exists.", getCurrentBlockId());
      } else {
        // This happens when there are any other cache stream close/cancel related errors (e.g.
        // server unreachable due to network partition, server busy due to alluxio worker is
        // busy, timeout due to congested network etc). But we want to proceed since we want
        // the user to continue reading when one Alluxio worker is having trouble.
        LOG.info(
                "Closing or cancelling the cache stream encountered IOExecption {}, reading from the "
                        + "regular stream won't be affected.", e.getMessage());
      }
    }
    mCurrentCacheStream = null;
  }
  /**
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    if (remaining() <= 0) {
      return -1;
    }
    return getBlockId(mPosition);
  }

  /**
   * Only updates {@link #mCurrentCacheStream}, {@link #mBlockInStream} and
   * to be in sync with the current block (i.e.
   * {@link #getCurrentBlockId()}).
   * If this method is called multiple times, the subsequent invokes become no-op.
   * Call this function every read and seek unless you are sure about the block streams are
   * up-to-date.
   *
   * @throws IOException if the next cache stream or block stream cannot be created
   */
  protected void updateStreams() throws IOException {
    long currentBlockId = getCurrentBlockId();
      // The following two function handle negative currentBlockId (i.e. the end of file)
      // correctly.
    if(shouldUpdateStreams(currentBlockId)) {
      updateStream(currentBlockId);
      updateCacheStream(currentBlockId);
      mStreamBlockId = currentBlockId;
    }
  }

  /**
   * Checks whether block instream and cache outstream should be updated.
   * This function is only called by {@link #updateStreams()}.
   *
   * @param currentBlockId cached result of {@link #getCurrentBlockId()}
   * @return true if the block stream should be updated
   */
  protected boolean shouldUpdateStreams(long currentBlockId) {
    if (mBlockInStream == null || currentBlockId != mStreamBlockId) {
      return true;
    }
    if (mCurrentCacheStream != null && inStreamRemaining() != cacheStreamRemaining()) {
      throw new IllegalStateException(
              String.format("BlockInStream and CacheStream is out of sync %d %d.",
                      inStreamRemaining(), cacheStreamRemaining()));
    }
    return inStreamRemaining() == 0;
  }


  /**
   * @param pos the pos
   * @return the block ID based on the pos
   */
  private long getBlockId(long pos) {
    int index = (int) (pos / mBlockSize);
    Preconditions
            .checkState(index < mStatus.getBlockIds().size(), PreconditionMessage.ERR_BLOCK_INDEX);
    return mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
  }


  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (retry.attempt()) {
      updateStreams();
      try {
        int result = mBlockInStream.read();
        if (result != -1) {
          mPosition++;
        }
        if (mCurrentCacheStream != null) {
          try {
            mCurrentCacheStream.write(result);
          } catch (IOException e) {
            handleCacheStreamIOException(e);
          }
        }
        return result;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        handleRetryableException(mBlockInStream, e);
        mBlockInStream = null;
      }
    }
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
    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (bytesLeft > 0 && mPosition != mLength && retry.attempt()) {
      updateStreams();
      try {
        int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
        if (bytesRead > 0) {
          if (mCurrentCacheStream != null) {
            try {
              mCurrentCacheStream.write(b, currentOffset, bytesRead);
            } catch (IOException e) {
              handleCacheStreamIOException(e);
            }
          }
          bytesLeft -= bytesRead;
          currentOffset += bytesRead;
          mPosition += bytesRead;
        }
        retry.reset();
        lastException = null;
      } catch (UnavailableException | ConnectException | DeadlineExceededException e) {
        lastException = e;
        handleRetryableException(mBlockInStream, e);
        mBlockInStream = null;
      }
    }
    if (lastException != null) {
      throw lastException;
    }

    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }



  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
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

    int lenCopy = len;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (len > 0 && retry.attempt()) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      BlockInStream stream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
      try {
        long offset = pos % mBlockSize;
        int bytesRead =
            stream.positionedRead(offset, b, off, (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
        retry.reset();
        lastException = null;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        handleRetryableException(stream, e);
        stream = null;
      } finally {
        closeBlockInStream(stream);
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return lenCopy - len;
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos,mLength);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition = pos;
      return;
    }

    long delta = pos - mPosition;
    if (delta <= mBlockInStream.remaining() && delta >= -mBlockInStream.getPos()) { // within block
      mBlockInStream.seek(mBlockInStream.getPos() + delta);
    } else { // close the underlying stream as the new position is no longer in bounds
      closeBlockInStream(mBlockInStream);
    }
    mPosition += delta;
  }




  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream(long blockId) throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
    }
    // blockId = -1 if mPos = EOF.
    if(blockId < 0){
      return;
    }

    // Create stream
    mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    mBlockInStream.seek(offset);
  }
  @Override
  public void close() throws IOException {
      updateStreams();
      closeBlockInStream(mBlockInStream);
      closeOrCancelCacheStream();
  }
  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      // Get relevant information from the stream.
      WorkerNetAddress dataSource = stream.getAddress();
      long blockId = stream.getId();
      BlockInStream.BlockInStreamSource blockSource = stream.getSource();
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
      if (blockSource == BlockInStream.BlockInStreamSource.LOCAL) {
        return;
      }

      // Send an async cache request to a worker based on read type and passive cache options.
//      boolean cache = mOptions.getOptions().getReadType().isCache();
//      boolean passiveCache = Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
//      long channelTimeout = Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
//      if (cache) {
//        WorkerNetAddress worker;
//        if (passiveCache && mContext.hasLocalWorker()) { // send request to local worker
//          worker = mContext.getLocalWorker();
//        } else { // send request to data source
//          worker = dataSource;
//        }
//        try {
//          // Construct the async cache request
//          long blockLength = mOptions.getBlockInfo(blockId).getLength();
//          Protocol.AsyncCacheRequest request =
//              Protocol.AsyncCacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
//                  .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
//                  .setSourceHost(dataSource.getHost()).setSourcePort(dataSource.getDataPort())
//                  .build();
//          Channel channel = mContext.acquireNettyChannel(worker);
//          try {
//            NettyRPCContext rpcContext =
//                NettyRPCContext.defaults().setChannel(channel).setTimeout(channelTimeout);
//            NettyRPC.fireAndForget(rpcContext, new ProtoMessage(request));
//          } finally {
//            mContext.releaseNettyChannel(worker, channel);
//          }
//        } catch (Exception e) {
//          LOG.warn("Failed to complete async cache request for block {} at worker {}: {}", blockId,
//              worker, e.getMessage());
//        }
//      }
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
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBool() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readInt() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public float readFloat() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public long readLong() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public double readDouble() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public String readString() throws IOException {
    throw new UnsupportedOperationException();

  }
///////////////


  @Override
  public int readByte(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean readBool(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public int readShort(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public int readInt(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public float readFloat(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public long readLong(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public double readDouble(long pos) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public String readString(long pos) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBytes(byte[] bytes, long pos) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] getByteBuffers() {
    throw new UnsupportedOperationException();
  }


}
