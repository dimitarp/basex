package org.basex.io.random;

import java.nio.ByteBuffer;

/**
 * This class represents a buffer.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
abstract class Buffer {
  /** Disk offset, or block position. */
  private long pos = -1L;
  /** Dirty flag. */
  private boolean dirty;

  /**
   * Get the byte value at the given position in the buffer.
   * @param index buffer posisiton
   * @return the byte value at the given position
   */
  public abstract byte get(int index);

  /**
   * Get reference to the underlying byte array.
   * @return byte array storing the buffer data
   */
  public byte[] getData() {
    return null;
  }

  /**
   * Get reference to the underlying {@link ByteBuffer}.
   * @return byte buffer storing the data
   */
  public ByteBuffer getByteBuffer() {
    return null;
  }

  public void setByteBuffer(final ByteBuffer b) {
  }

  /**
   * Set the byte value at the given position in the buffer.
   * @param index buffer position
   * @param b byte value to set
   */
  public final void set(int index, byte b) {
    _set(index, b);
    dirty = true;
  }

  /**
   * Get the position in the file loaded in this buffer.
   * @return file position
   */
  public final long getPos() {
    return pos;
  }

  /**
   * Set the position in the file loaded in this buffer.
   * @param p new file position
   */
  public final void setPos(long p) {
    pos = p;
  }

  /**
   * Shows if the buffer is dirty.
   * @return {@code true} if buffer data is not stored to disk
   */
  public final boolean isDirty() {
    return dirty;
  }

  /**
   * Set the dirty flag.
   * @param d dirty flag
   */
  public final void setDirty(boolean d){
    dirty = d;
  }

  /**
   * Copy bytes from buffer to a byte array.
   * @param pos buffer position from which to start copying (inclusive)
   * @param dest byte array to which to copy the data
   * @param destPos position in the array where to start writing (inclusive)
   * @param length number of bytes to copy
   */
  public abstract void copyTo(int pos, byte[] dest, int destPos, int length);

  /**
   * Copy bytes from a byte array to buffer.
   * @param pos buffer position from which to start writing (inclusive)
   * @param src byte array from which to copy the data
   * @param srcPos position in the array where to start copying (inclusive)
   * @param length number of bytes to copy
   */
  public final void copyFrom(int pos, byte[] src, int srcPos, int length) {
    _copyFrom(pos, src, srcPos, length);
    dirty = true;
  }

  /**
   * Move data within the buffer.
   * @param pos position from which to start reading data
   * @param off offset to which to start writing data
   * @param size number of bytes to move
   */
  public final void move(final int pos, final int off, final int size) {
    _move(pos, off, size);
    dirty = true;
  }

  /**
   * Reads a byte value and returns it as an integer value.
   * @param i buffer position
   * @return byte value
   */
  public int read1(int i) {
    return get(i) & 0xFF;
  }

  /**
   * Reads a short value and returns it as an integer value.
   * @param i buffer position
   * @return short value
   */
  public int read2(int i) {
    return ((get(i)     & 0xFF) << 8) +
            (get(i + 1) & 0xFF);
  }

  /**
   * Reads an integer value.
   * @param i buffer position
   * @return integer value
   */
  public int read4(int i) {
    return ((get(i)     & 0xFF) << 24) +
           ((get(i + 1) & 0xFF) << 16) +
           ((get(i + 2) & 0xFF) << 8) +
            (get(i + 3) & 0xFF);
  }

  /**
   * Reads a 5-byte value and returns it as a long value.
   * @param i buffer position
   * @return long value
   */
  public long read5(int i) {
    return ((long) (get(i)     & 0xFF) << 32) +
           ((long) (get(i + 1) & 0xFF) << 24) +
                  ((get(i + 2) & 0xFF) << 16) +
                  ((get(i + 3) & 0xFF) << 8) +
                   (get(i + 4) & 0xFF);
  }

  /**
   * Writes a byte value to the specified position.
   * @param i buffer position
   * @param v value to be written
   */
  public void write1(int i, int v) {
    _set(i, (byte) v);
    dirty = true;
  }

  /**
   * Writes a short value to the specified position.
   * @param i buffer position
   * @param v value to be written
   */
  public void write2(int i, int v) {
    _set(i,     (byte) (v >>> 8));
    _set(i + 1, (byte) v);
    dirty = true;
  }

  /**
   * Writes an integer value to the specified position.
   * @param i buffer position
   * @param v value to be written
   */
  public void write4(int i, int v) {
    _set(i,     (byte) (v >>> 24));
    _set(i + 1, (byte) (v >>> 16));
    _set(i + 2, (byte) (v >>> 8));
    _set(i + 3, (byte) v);
    dirty = true;
  }

  /**
   * Writes a 5-byte value to the specified position.
   * @param i buffer position
   * @param v value to be written
   */
  public void write5(int i, long v) {
    _set(i, (byte) (v >>> 32));
    _set(i + 1, (byte) (v >>> 24));
    _set(i + 2, (byte) (v >>> 16));
    _set(i + 3, (byte) (v >>> 8));
    _set(i + 4, (byte) v);
    dirty = true;
  }

  /**
   * Set the byte value at the given position in the underlying buffer.
   * @param i buffer posisiton
   * @param b byte value to set
   */
  protected abstract void _set(int i, byte b);

  /**
   * Copy bytes from a byte array to the underlying buffer.
   * @param pos buffer position from which to start writing (inclusive)
   * @param src byte array from which to copy the data
   * @param srcPos position in the array where to start copying (inclusive)
   * @param length number of bytes to copy
   */
  protected abstract void _copyFrom(int pos, byte[] src, int srcPos, int length);

  /**
   * Move data within the underlying buffer.
   * @param pos position from which to start reading data
   * @param off offset to which to start writing data
   * @param size number of bytes to move
   */
  protected abstract void _move(final int pos, final int off, final int size);
}
