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

import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.TestBlockInStreamV2;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;

/**
 * Tests for the {@link FileInStreamV2}; class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, UnderFileSystem.class})
public abstract class Input4BaseV2Test {

  protected static final long STEP_LENGTH = 100L;
  protected static final long UNIT_LENGTH = 350L;
  protected static final long NUM_STREAMS = 1;

  protected AlluxioBlockStore mBlockStore;
  protected FileSystemContext mContext;
  protected FileInfo mInfo;
  protected URIStatus mStatus;
  protected List<TestBlockInStreamV2> mInStreams;
  protected FileInStream mTestStream;
  private BlockInStreamSource mBlockSource;

  public int unitSize(){
    return 1;
  }
  protected String dataPath() {
    return "/tmp/alluxio/data/" + this.getClass().getName();
  }

  protected abstract byte[] data(int v);

  /**
   * Sets up the context and streams before a test runs.
   *
   * @throws AlluxioException when the worker ufs operations fail
   * @throws IOException when the read and write streams fail
   */
  @Before
  public void before() throws Exception {
    mInfo = genFileInfo();

    ClientTestUtils.setSmallBufferSizes();

    mContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.when(mContext.getLocalWorker()).thenReturn(new WorkerNetAddress());
    mBlockStore = Mockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);
    PowerMockito.when(mBlockStore.getEligibleWorkers()).thenReturn(new ArrayList<>());

    // Set up BufferedBlockInStreams and caching streams
    mInStreams = new ArrayList<>();
    List<Long> blockIds = new ArrayList<>();
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    for (int i = 0; i < NUM_STREAMS; i++) {
      blockIds.add((long) i);
      FileBlockInfo fbInfo = new FileBlockInfo().setBlockInfo(new BlockInfo().setBlockId(i));
      fileBlockInfos.add(fbInfo);
      final byte[] input = data(i);
      TestBlockInStreamV2.write(dataPath(), input);
      TestBlockInStreamV2.mPath = dataPath();
      mInStreams.add(new TestBlockInStreamV2(i, input.length));
      Mockito.when(mBlockStore.getEligibleWorkers())
          .thenReturn(Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress(), 0, 0)));
      Mockito.when(mBlockStore.getInStream(Mockito.eq((long) i), Mockito.any(InStreamOptions
          .class), any()))
          .thenAnswer(new Answer<BlockInStream>() {
            @Override
            public BlockInStream answer(InvocationOnMock invocation) throws Throwable {
              long blockId = (Long) invocation.getArguments()[0];
              return mInStreams.get((int) blockId).isClosed() ? new TestBlockInStreamV2(
                  blockId, input.length) : mInStreams.get((int) blockId);
            }
          });
    }
    mInfo.setBlockIds(blockIds);
    mInfo.setFileBlockInfos(fileBlockInfos);
    mStatus = new URIStatus(mInfo);
    mTestStream = gen();
  }

  public FileInfo genFileInfo() {
    return new FileInfo().setBlockSizeBytes(STEP_LENGTH).setLength(UNIT_LENGTH * unitSize());
  }

  public FileInStream gen() {
    OpenFileOptions readOptions = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
    return new FileInStreamV2(mStatus, new InStreamOptions(mStatus, readOptions), mContext);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient();
  }


}
