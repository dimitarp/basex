package org.basex.io.random;

import java.nio.ByteBuffer;

/**
 * {@link Buffer} implementation using {@link ByteBuffer}.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
final class BlockBuffer extends Buffer {
  /** Buffer data. */
  private ByteBuffer buffer;

  /**
   * Constructor.
   * @param size buffer size
   */
  BlockBuffer(int size) {
    buffer = ByteBuffer.allocateDirect(size);
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return buffer;
  }

  @Override
  public void setByteBuffer(final ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public byte get(int index) {
    return buffer.get(index);
  }

  @Override
  protected void _set(int index, byte b) {
    buffer.put(index, b);
  }

  @Override
  public void copyTo(int pos, byte[] dest, int destPos, int length) {
    buffer.position(pos);
    buffer.get(dest, destPos, length);
    buffer.rewind();
  }

  @Override
  public int read2(int i) {
    return buffer.getShort(i);
  }

  @Override
  public int read4(int i) {
    return buffer.getInt(i);
  }

  @Override
  public long read5(int i) {
    return (((long) buffer.getInt(i)) << 8) + read1(i + 4);
  }

  @Override
  public void write2(int i, int v) {
    buffer.putShort(i, (short) v);
    setDirty(true);
  }

  @Override
  public void write4(int i, int v) {
    buffer.putInt(i, v);
    setDirty(true);
  }

  @Override
  public void write5(int i, long v) {
    buffer.putInt(i, (int) (v >>> 8));
    buffer.put(i + 4, (byte) v);
    setDirty(true);
  }

  @Override
  protected void _copyFrom(int pos, byte[] src, int srcPos, int length) {
    buffer.position(pos);
    buffer.put(src, srcPos, length);
    buffer.rewind();
  }

  @Override
  protected void _move(int pos, int off, int size) {
    buffer.position(pos);
    ByteBuffer slice = buffer.slice();
    slice.limit(size);

    buffer.position(pos + off);
    buffer.put(slice);
    buffer.rewind();
  }
}
