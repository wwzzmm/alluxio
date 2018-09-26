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

package alluxio.master.block;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link BlockId} class.
 */
public final class BlockIdTest {

  private static final long MAX_SEQUENCE_NUMBER = 16777215L;

  /**
   * Tests that the {@link BlockId#createBlockId(long, long)} method works correctly when creating a
   * block with the maximum sequence number.
   */
  @Test
  public void createBlockIdWithMaxSequenceNumber() {
    Assert.assertEquals(33554431L, BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER,
        BlockId.createBlockId(0, BlockId.getMaxSequenceNumber()));
    Assert.assertEquals(4294967295L, BlockId.createBlockId(255, BlockId.getMaxSequenceNumber()));
  }

  /**
   * Tests the {@link BlockId#createBlockId(long, long)} method.
   */
  @Test
  public void createBlockId() {
    Assert.assertEquals(16797216L, BlockId.createBlockId(1, 20000L));
    Assert.assertEquals(20000L, BlockId.createBlockId(0, 20000L));
    Assert.assertEquals(2071248101952L, BlockId.createBlockId(123456, 123456L));
  }

  /**
   * Tests the {@link BlockId#getContainerId(long)} and {@link BlockId#getSequenceNumber(long)}
   * methods.
   */
  @Test
  public void getContainerIdAndSequenceNumber() {
    Assert.assertEquals(1L, BlockId.getContainerId(33554431L));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(33554431L));
    Assert.assertEquals(255L, BlockId.getContainerId(4294967295L));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(4294967295L));
    Assert.assertEquals(123456L, BlockId.getContainerId(2071248101952L));
    Assert.assertEquals(123456L, BlockId.getSequenceNumber(2071248101952L));
  }

  /**
   * Tests the {@link BlockId#getMaxSequenceNumber()} method.
   */
  @Test
  public void getMaxSequenceNumber() {
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getMaxSequenceNumber());
  }


  @Test
  public void getContainerId(){

//    printId(16777216000l);
//    printId(16793993215l);
//    printId(3081118941183l);
//    printId(3081102163968l);
//    printId(3081219604479l);
//    printId(3081202827264l);
//    printId(21869772144644l);
//    printId(2225480925184l);
//    printId(2256418111488l);

    printId(16777216000l);
    printId(18706595840l);
    printId(18689818624l);
    printId(18673041408l);
    printId(18656264192l);
    printId(18723373056l);
  }

  private void printId(long blockId){
    System.out.println(BlockId.getContainerId(blockId)+" "+BlockId.getSequenceNumber(blockId));
  }

}
