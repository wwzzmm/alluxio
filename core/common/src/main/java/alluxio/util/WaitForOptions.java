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

package alluxio.util;

import com.google.common.base.Objects;

/**
 * Options for the {@link CommonUtils#waitFor} method.
 */
public final class WaitForOptions {
  static final int DEFAULT_INTERVAL = 20;
  public static final int NEVER = -1;

  /** How often to check for completion. */
  private int mIntervalMs;
  /** How long to wait before giving up. */
  private int mTimeoutMs;
  /** Whether to throw an exception on timeout. If false, we will return false instead. */
  private boolean mThrowOnTimeout;

  private WaitForOptions() {}

  /**
   * @return the default instance of {@link WaitForOptions}
   */
  public static WaitForOptions defaults() {
    return new WaitForOptions().setInterval(DEFAULT_INTERVAL).setTimeoutMs(NEVER)
        .setThrowOnTimeout(true);
  }

  /**
   * @return the internal
   */
  public int getInterval() {
    return mIntervalMs;
  }

  /**
   * @return the timeout
   */
  public int getTimeoutMs() {
    return mTimeoutMs;
  }

  /**
   * @return whether to throw an exception on timeout
   */
  public boolean isThrowOnTimeout() {
    return mThrowOnTimeout;
  }

  /**
   * @param intervalMs the interval to use (in milliseconds)
   * @return the updated options object
   */
  public WaitForOptions setInterval(int intervalMs) {
    mIntervalMs = intervalMs;
    return this;
  }

  /**
   * @param timeoutMs the timeout to use (in milliseconds)
   * @return the updated options object
   */
  public WaitForOptions setTimeoutMs(int timeoutMs) {
    mTimeoutMs = timeoutMs;
    return this;
  }

  /**
   * @param throwOnTimeout whether to throw an exception on timeout
   * @return the updated options object
   */
  public WaitForOptions setThrowOnTimeout(boolean throwOnTimeout) {
    mThrowOnTimeout = throwOnTimeout;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WaitForOptions)) {
      return false;
    }
    WaitForOptions that = (WaitForOptions) o;
    return mIntervalMs == that.mIntervalMs
        && mTimeoutMs == that.mTimeoutMs
        && mThrowOnTimeout == that.mThrowOnTimeout;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mIntervalMs, mTimeoutMs, mThrowOnTimeout);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("interval", mIntervalMs)
        .add("timeout", mTimeoutMs)
        .add("throwOnTimeout", mThrowOnTimeout)
        .toString();
  }
}
