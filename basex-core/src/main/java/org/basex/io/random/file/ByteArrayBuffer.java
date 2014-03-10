package org.basex.io.random.file;

import org.basex.util.Array;

/**
 * Buffer implementation using a simple byte array.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
final class ByteArrayBuffer extends Buffer {
  /** Buffer data. */
  private final byte[] data;

  /**
   * Constructor.
   * @param size buffer size
   */
  ByteArrayBuffer(int size) {
    data = new byte[size];
  }

  @Override
  public int read2(int i) {
    return ((get(i)     & 0xFF) << 8) +
            (get(i + 1) & 0xFF);
  }

  @Override
  public int read4(int i) {
    return ((get(i)     & 0xFF) << 24) +
           ((get(i + 1) & 0xFF) << 16) +
           ((get(i + 2) & 0xFF) << 8) +
            (get(i + 3) & 0xFF);
  }

  @Override
  public long read5(int i) {
    return ((long) (get(i)     & 0xFF) << 32) +
           ((long) (get(i + 1) & 0xFF) << 24) +
                  ((get(i + 2) & 0xFF) << 16) +
                  ((get(i + 3) & 0xFF) << 8) +
                   (get(i + 4) & 0xFF);
  }

  @Override
  public byte[] getData() {
    return data;
  }

  @Override
  public byte get(int index) {
    return data[index];
  }

  @Override
  public void copyTo(int pos, byte[] dest, int destPos, int length) {
    System.arraycopy(data, pos, dest, destPos, length);
  }

  @Override
  protected void _set(int index, byte b) {
    data[index] = b;
  }

  @Override
  protected void _copyFrom(int pos, byte[] src, int srcPos, int length) {
    System.arraycopy(src, srcPos, data, pos, length);
  }

  @Override
  protected void _move(int pos, int off, int size) {
    Array.move(data, pos, off, size);
  }
}
