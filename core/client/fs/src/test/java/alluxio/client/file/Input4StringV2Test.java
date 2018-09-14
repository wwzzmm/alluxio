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
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the {@link FileInStreamV2}; class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, UnderFileSystem.class})
public class Input4StringV2Test extends Input4BaseV2Test {
  protected long UNIT_LENGTH = 50L;

  @Override
  protected byte[] data(int i) {
    return BufferUtils
        .getIncreasingStringByteArray((int) (i * UNIT_LENGTH), (int) UNIT_LENGTH);
  }
  protected List<String> content = values((int) UNIT_LENGTH);

  @Override
  public FileInfo genFileInfo() {
    return new FileInfo().setBlockSizeBytes(STEP_LENGTH).setLength(arrayPos((int) UNIT_LENGTH));
  }
  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleIntRead() throws Exception {
    int l = arrayPos((int) UNIT_LENGTH);
    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(l - arrayPos(i), mTestStream.remaining());
      Assert.assertEquals(value(i), mTestStream.readString());
      Assert.assertEquals(l - arrayPos(i + 1), mTestStream.remaining());
//      System.out.println(i);
    }
    mTestStream.close();
  }

  protected int arrayPos(int pos) {
    return pos == 0 ? 0 : pos * 4 + (pos - 1) * (pos) / 2;
  }

  protected List<String> values(int len) {
    List<String> r = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      r.add(value(i, false));
    }
    return r;
  }

  protected String value(int pos) {
    return content.get(pos);
  }

  private String value(int pos, boolean flag) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < pos; i++) {
      if (i % 4 == 0) {
        sb.append('a');
      }
      if (i % 4 == 1) {
        sb.append('b');
      }
      if (i % 4 == 2) {
        sb.append('c');
      }
      if (i % 4 == 3) {
        sb.append('d');
      }
    }
    return sb.length() == 0 ? null : sb.toString();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleIntReadPos() throws Exception {
    int l = arrayPos((int) UNIT_LENGTH);

    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(l, mTestStream.remaining());
      Assert.assertEquals(value(i), mTestStream.readString(arrayPos(i)));
      Assert.assertEquals(l, mTestStream.remaining());

    }
    mTestStream.close();
  }

  @Test
  public void randomRead() throws Exception {

    int time = 10000;
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      int p = (int) (Math.abs(generator.nextInt() / 2) % UNIT_LENGTH);
      Assert.assertEquals(value(p), mTestStream.readString(arrayPos(p)));
    }
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void sequenceSeek() throws IOException {

    long fl = UNIT_LENGTH;
    for (int i = 0; i < fl; i++) {
      mTestStream.seek((long) arrayPos(i));
      Assert.assertEquals(value(i), mTestStream.readString());
    }
    Assert.assertEquals(0, mTestStream.remaining());
  }

  @Test
  public void randomSeek() throws IOException {

    int time = 1000_0;
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      long p = Math.abs(generator.nextInt() / 2) % UNIT_LENGTH;
      mTestStream.seek(arrayPos((int) p));
      Assert.assertEquals(value((int) p), mTestStream.readString());
    }
  }

  @Test
  public void sequenceSkip() throws IOException {

    long fl = UNIT_LENGTH;
    for (int i = 0; i < fl / 2; i++) {
      mTestStream.skip((long) (4 + i * 2));
      Assert.assertEquals(value(i * 2 + 1), mTestStream.readString());
    }
    Assert.assertEquals(0, mTestStream.remaining());

  }

  @Test
  public void randomSkip() throws IOException {

    int time = 10000;
    RandomGenerator generator = new JDKRandomGenerator();

    int pos = 0;
    for (int i = 0; i < time; i++) {
      int tPos;
      int step;
      do {
        step = (generator.nextInt() / 2) % 100;
        tPos = pos + step;
      } while (tPos * unitSize() >= UNIT_LENGTH * unitSize() || tPos < 0);
      if (step < 0) {
        mTestStream.seek(arrayPos(tPos));
      } else {
        mTestStream.skip((long) (arrayPos(tPos) - arrayPos(pos)));
      }
      Assert.assertEquals(value(tPos), mTestStream.readString());
      pos += step + 1;
    }
  }


}
