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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * input interface instead of {@link java.io.InputStream}
 * <p>
 * corresponding to {@link Output}
 */
public interface Input extends Closeable {

  /**
   * Read one byte at current position
   */
  int readByte() throws IOException;

  /**
   * Read one byte at current position ,return the
   * boolean value of comparing this byte value with one
   */
  boolean readBool() throws IOException;

  /**
   * Read two bytes at current position,converting to short value
   */
  int readShort() throws IOException;

  /**
   * Read four bytes at current position,converting to int value
   */
  int readInt() throws IOException;

  /**
   * Read four bytes at current position,converting to float value
   */
  float readFloat() throws IOException;

  /**
   * Read eight bytes at current position,converting to long value
   */
  long readLong() throws IOException;

  /**
   * Read eight bytes at current position,converting to double value
   */
  double readDouble() throws IOException;

  /**
   * Read string value using below rules.
   *
   * 1.Read int value at the current buffer position, indicating the offset of the string 2.Transfer
   * bytes from input buffer into a temp byte array,starting at current position and using the
   * corresponding offset value.Then compose this temp byte array into string value as finally
   * value,in the current byte order
   */
  String readString() throws IOException;


  /**
   * Close the input
   */
  void close() throws IOException;


  /**
   * Read one byte at given position without increments the position of
   * the buffer
   */
  int readByte(int pos) throws IOException;

  /**
   * Read one byte at given position without increments the position of the buffer ,boolean value
   * of comparing this byte value with one
   */
  boolean readBool(int pos) throws IOException;

  /**
   * Read two bytes at given position,converting to float value without increments the position of
   * the buffer
   */
  int readShort(int pos) throws IOException;

  /**
   * Read four bytes at given position,converting to int value without increments the position of
   * the buffer
   */
  int readInt(int pos) throws IOException;

  /**
   * Read four bytes at given position,converting to float value without increments the position of
   * the buffer
   */
  float readFloat(int pos) throws IOException;

  /**
   * Read eight bytes at given position,converting to long value without increments the position of
   * the buffer
   */
  long readLong(int pos) throws IOException;

  /**
   * Read eight bytes at given position,converting to double value without increments the position
   * of the buffer
   */
  double readDouble(int pos) throws IOException;


  /**
   * Read string value using below rules.
   *
   * 1.Read int value at the given position, indicating the offset of the string 2.Transfer bytes
   * into a temp byte array,starting at given position plus another four positions and at the
   * corresponding offset value.Then compose this temp byte array into string value as finally
   * value,in the current byte order.This method doesn't increment the position of the buffer
   */
  String readString(int pos) throws IOException;

  void readBytes(byte[] bytes, int pos) throws IOException;

  ByteBuffer getByteBuffer();
}
