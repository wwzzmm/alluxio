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

import alluxio.annotation.PublicApi;
import alluxio.client.file.options.InStreamOptions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * 空文件。V2和V3的FileInStream是不支持空文件操作。因为在构造时便进行初始化，减少读取时候的判断。所有在空文件的情况下，BlockSize为0导致报错。
 */
@PublicApi
@NotThreadSafe
public class FileInStreamEmpty extends FileInStream {

  public FileInStreamEmpty(URIStatus status, InStreamOptions options,
                           FileSystemContext context) {
    super(status, options, context);
  }

  @Override
  public int readByte() throws IOException {
    return -1;
  }

  @Override
  public boolean readBool() throws IOException {
    throw new IndexOutOfBoundsException();
  }

  @Override
  public int readShort() throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public int readInt() throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public float readFloat() throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public long readLong() throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public double readDouble() throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public String readString() throws IOException {
    throw new IndexOutOfBoundsException();

  }
///////////////


  @Override
  public int readByte(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public boolean readBool(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public int readShort(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public int readInt(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public float readFloat(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public long readLong(int pos) throws IOException {
    throw new IndexOutOfBoundsException();

  }

  @Override
  public double readDouble(int pos) throws IOException {
    throw new IndexOutOfBoundsException();
  }

  @Override
  public String readString(int pos) throws IOException {
    throw new IndexOutOfBoundsException();
  }
  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    if(bytes.length!=0){
      throw new IndexOutOfBoundsException();
    }
  }

}
