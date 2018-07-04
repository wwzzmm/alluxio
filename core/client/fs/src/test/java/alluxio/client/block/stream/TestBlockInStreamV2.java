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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * A {@link BlockInStream} which reads from the given byte array. The stream is able to track how
 * much bytes that have been read from the extended BlockInStream.
 */
public class TestBlockInStreamV2 extends BlockInStreamV2 {
  public static String mPath;
  /** A field tracks how much bytes read. */
  private int mBytesRead;
  private boolean mClosed;

  public TestBlockInStreamV2(long id, long length) throws IOException {
    super(null, null, id, length, null, 0, null);
    mBytesRead = 0;
  }

  public static void write(String path, byte[] data) throws IOException {
    Closer closer = Closer.create();
    MappedByteBuffer buffer = null;
    File file = new File(path);
    if (!file.getParentFile().exists()) {
      FileUtils.forceMkdir(file.getParentFile());
    }
    try {
      RandomAccessFile f = closer.register(new RandomAccessFile(
          file.getAbsoluteFile(), "rw"));
      FileChannel fc = closer.register(f.getChannel());
      buffer = fc.map(MapMode.READ_WRITE, 0, data.length);
      buffer.put(data, 0, data.length);
    } catch (Exception e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
      BufferUtils.cleanDirectBuffer(buffer);
    }
  }

  @Override
  protected String getPath(WorkerNetAddress address, FileSystemContext context,
                           InStreamOptions options) throws IOException {
    return mPath;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = super.read(b, off, len);
    if (bytesRead <= 0) {
      return bytesRead;
    }
    mBytesRead += bytesRead;
    return bytesRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    int bytesRead = super.positionedRead(pos, b, off, len);
    if (bytesRead <= 0) {
      return bytesRead;
    }
    mBytesRead += bytesRead;
    return bytesRead;
  }

  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() throws IOException {
    mClosed = true;
    super.close();
  }

  /**
   * @return how many bytes been read
   */
  public int getBytesRead() {
    return mBytesRead;
  }

  /**
   * Factory class to create {@link TestPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final byte[] mData;
    private final boolean mShortCircuit;

    /**
     * Creates an instance of {@link LocalFilePacketReader.Factory}.
     *
     * @param data the data to serve
     */
    public Factory(byte[] data, boolean shortCircuit) {
      mData = data;
      mShortCircuit = shortCircuit;
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new TestPacketReader(mData, offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return mShortCircuit;
    }

    @Override
    public void close() {
    }
  }
}
