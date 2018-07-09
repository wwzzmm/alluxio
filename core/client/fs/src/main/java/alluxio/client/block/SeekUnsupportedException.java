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
package alluxio.client.block;

/**
 * This class created on 2017/12/22.
 *
 * @author Connery
 */
public class SeekUnsupportedException extends RuntimeException {
  public SeekUnsupportedException() {
  }

  public SeekUnsupportedException(String message) {
    super(message);
  }

  public SeekUnsupportedException(String message, Throwable cause) {
    super(message, cause);
  }

  public SeekUnsupportedException(Throwable cause) {
    super(cause);
  }

  public SeekUnsupportedException(String message, Throwable cause, boolean enableSuppression,
                                  boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
