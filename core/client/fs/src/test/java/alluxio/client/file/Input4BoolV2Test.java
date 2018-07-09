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

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.util.ClientTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Tests for the {@link FileInStreamV2}; class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, UnderFileSystem.class})
public class Input4BoolV2Test extends Input4BaseV2Test {

  private long
  getBlockLength(int streamId) {
    return streamId == NUM_STREAMS - 1 ? 50 : STEP_LENGTH;
  }


  @Override
  protected byte[] data(int i) {
    return BufferUtils
        .getIncreasingBoolByteArray((int) (i * UNIT_LENGTH), (int) UNIT_LENGTH);
  }



  @After
  public void after() {
    ClientTestUtils.resetClient();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleReadPos() throws Exception {
    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(i % 2 == 1, mTestStream.readBool(i * unitSize()));
      Assert.assertEquals(UNIT_LENGTH * unitSize(), mTestStream.remaining());

    }
    mTestStream.close();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleRead() throws Exception {
    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(i % 2 == 1, mTestStream.readBool());
      Assert.assertEquals(UNIT_LENGTH * unitSize() - unitSize() - i * unitSize(),
          mTestStream.remaining());

    }
    mTestStream.close();
  }

  @Test
  public void randomRead() throws Exception {

    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();
    for (int i = 0; i < time; i++) {
      int p = (int) (Math.abs(generator.nextInt() / 2) % UNIT_LENGTH);
      Assert.assertEquals(p % 2 == 1, mTestStream.readBool(p * unitSize()));
    }
  }


  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void sequenceSeek() throws IOException {

    long fl = UNIT_LENGTH;
    for (int i = 0; i < fl; i++) {
      mTestStream.seek(i * unitSize());
      Assert.assertEquals(i % 2 == 1, (boolean) mTestStream.readBool());
    }
    Assert.assertEquals(0, mTestStream.remaining());
  }

  @Test
  public void randomSeek() throws IOException {

    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      long p = Math.abs(generator.nextInt() / 2) % UNIT_LENGTH;
      mTestStream.seek(p * unitSize());
      Assert.assertEquals(p % 2 == 1, (boolean) mTestStream.readBool());
    }
  }

  @Test
  public void sequenceSkip() throws IOException {

    int v = 0;
    boolean[] con = BufferUtils
        .getIncreasingBoolArray(0, (int) UNIT_LENGTH);
    for (int i = 0; i < UNIT_LENGTH / 2; i++) {
      mTestStream.skip(unitSize());
      v += 1;
      Assert.assertEquals(con[v], (boolean) mTestStream.readBool());
      v += 1;
    }
    Assert.assertEquals(0, mTestStream.remaining());

  }

  @Test
  public void randomSkip() throws IOException {
    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    boolean[] con = BufferUtils
        .getIncreasingBoolArray(0, (int) UNIT_LENGTH);
    int pos = 0;
    for (int i = 0; i < time; i++) {
      int tPos;
      int step;
      do {
        step = (generator.nextInt() / 2) % 100;
        tPos = pos + step;
      } while (tPos * unitSize() >= UNIT_LENGTH * unitSize() || tPos < 0);
      if (step < 0) {
        mTestStream.seek(tPos * unitSize());
      } else {
        mTestStream.skip(step * unitSize());
      }
      Assert.assertEquals(con[tPos], (boolean) mTestStream.readBool());
      pos += step + 1;
    }
  }


}
