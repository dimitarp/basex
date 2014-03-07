package org.basex.io.random;

import java.io.*;

import org.basex.io.*;
import org.basex.util.*;

/**
 * This class allows positional read and write access to a database file.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class DataAccess {
  /** Reference to the data input stream. */
  private final BlockFileAccess file;
  /** File length. */
  private long len;
  /** Changed flag. */
  private boolean changed;
  /** Offset. */
  private int off;

  /**
   * Constructor, initializing the file reader.
   * @param fl the file to be read
   * @throws IOException I/O Exception
   */
  public DataAccess(final IOFile fl) throws IOException {
    BlockFileAccess f = null;
    try {
      f = BlockFileAccess.open(fl);
      len = f.length();
    } catch(final IOException ex) {
      if(f != null) f.close();
      throw ex;
    }
    file = f;
    cursor(0);
  }

  /**
   * Flushes the buffered data.
   */
  public synchronized void flush() {
    try {
      file.flush();
      if(changed) {
        file.setLength(len);
        changed = false;
      }
    } catch(final IOException ex) {
      Util.stack(ex);
    }
  }

  /**
   * Closes the data access.
   */
  public synchronized void close() {
    flush();
    try {
      file.close();
    } catch(final IOException ex) {
      Util.stack(ex);
    }
  }

  /**
   * Returns the current file position.
   * @return position in the file
   */
  public long cursor() {
    return buffer(false).getPos() + off;
  }

  /**
   * Sets the file length.
   * @param l file length
   */
  synchronized void length(final long l) {
    changed |= l != len;
    len = l;
  }

  /**
   * Returns the file length.
   * @return file length
   */
  public long length() {
    return len;
  }

  /**
   * Checks if more bytes can be read.
   * @return result of check
   */
  public boolean more() {
    return cursor() < len;
  }

  /**
   * Reads the next byte.
   * @return next byte
   */
  public int read() {
    return buffer(off == IO.BLOCKSIZE).read1(off++);
  }

  /**
   * Reads a byte value from the specified position.
   * @param p position
   * @return integer value
   */
  public synchronized byte read1(final long p) {
    cursor(p);
    return read1();
  }

  /**
   * Reads a byte value.
   * @return integer value
   */
  public synchronized byte read1() {
    return (byte) read();
  }

  /**
   * Reads an integer value from the specified position.
   * @param p position
   * @return integer value
   */
  public synchronized int read4(final long p) {
    cursor(p);
    return read4();
  }

  /**
   * Reads an integer value.
   * @return integer value
   */
  public synchronized int read4() {
    return (read() << 24) + (read() << 16) + (read() << 8) + read();
  }

  /**
   * Reads a 5-byte value from the specified file offset.
   * @param p position
   * @return long value
   */
  public synchronized long read5(final long p) {
    cursor(p);
    return read5();
  }

  /**
   * Reads a 5-byte value.
   * @return long value
   */
  public synchronized long read5() {
    return ((long) read() << 32) + ((long) read() << 24) +
      (read() << 16) + (read() << 8) + read();
  }

  /**
   * Reads a {@link Num} value from disk.
   * @param p text position
   * @return read num
   */
  public synchronized int readNum(final long p) {
    cursor(p);
    return readNum();
  }

  /**
   * Reads a token from disk.
   * @param p text position
   * @return text as byte array
   */
  public synchronized byte[] readToken(final long p) {
    cursor(p);
    return readToken();
  }

  /**
   * Reads the next token from disk.
   * @return text as byte array
   */
  public synchronized byte[] readToken() {
    final int l = readNum();
    return readBytes(l);
  }

  /**
   * Reads a number of bytes from the specified offset.
   * @param p position
   * @param l length
   * @return byte array
   */
  public synchronized byte[] readBytes(final long p, final int l) {
    cursor(p);
    return readBytes(l);
  }

  /**
   * Reads a number of bytes.
   * @param n length
   * @return byte array
   */
  public synchronized byte[] readBytes(final int n) {
    int l = n;
    int ll = IO.BLOCKSIZE - off;
    final byte[] b = new byte[l];

    buffer(false).copyTo(off, b, 0, Math.min(l, ll));
    if(l > ll) {
      l -= ll;
      while(l > IO.BLOCKSIZE) {
        buffer(true).copyTo(0, b, ll, IO.BLOCKSIZE);
        ll += IO.BLOCKSIZE;
        l -= IO.BLOCKSIZE;
      }
      buffer(true).copyTo(0, b, ll, l);
    }
    off += l;
    return b;
  }

  /**
   * Sets the disk cursor.
   * @param p read position
   */
  public void cursor(final long p) {
    off = (int) (p & IO.BLOCKSIZE - 1);
    file.readBlock(p - off);
  }

  /**
   * Reads the next compressed number and returns it as integer.
   * @return next integer
   */
  public synchronized int readNum() {
    final int v = read();
    switch(v & 0xC0) {
    case 0:
      return v;
    case 0x40:
      return (v - 0x40 << 8) + read();
    case 0x80:
      return (v - 0x80 << 24) + (read() << 16) + (read() << 8) + read();
    default:
      return (read() << 24) + (read() << 16) + (read() << 8) + read();
    }
  }

  /**
   * Writes the next byte.
   * @param b byte to be written
   */
  public void write(final int b) {
    final Buffer bf = buffer(off == IO.BLOCKSIZE);
    bf.set(off++, (byte) b);
    final long nl = bf.getPos() + off;
    if(nl > len) length(nl);
  }

  /**
   * Writes a 5-byte value to the specified position.
   * @param p position in the file
   * @param v value to be written
   */
  public void write5(final long p, final long v) {
    cursor(p);
    write((byte) (v >>> 32));
    write((byte) (v >>> 24));
    write((byte) (v >>> 16));
    write((byte) (v >>> 8));
    write((byte) v);
  }

  /**
   * Writes an integer value to the specified position.
   * @param p write position
   * @param v byte array to be appended
   */
  public void write4(final long p, final int v) {
    cursor(p);
    write4(v);
  }

  /**
   * Writes an integer value to the current position.
   * @param v value to be written
   */
  public void write4(final int v) {
    write(v >>> 24);
    write(v >>> 16);
    write(v >>>  8);
    write(v);
  }

  /**
   * Write a value to the file.
   * @param p write position
   * @param v value to be written
   */
  public void writeNum(final long p, final int v) {
    cursor(p);
    writeNum(v);
  }

  /**
   * Writes integers to the file in compressed form.
   * @param p write position
   * @param v integer values
   */
  public void writeNums(final long p, final int[] v) {
    cursor(p);
    writeNum(v.length);
    for(final int n : v) writeNum(n);
  }

  /**
   * Appends integers to the file in compressed form.
   * @param v integer values
   * @return the position in the file where the values have been written
   */
  public long appendNums(final int[] v) {
    final long end = len;
    writeNums(end, v);
    return end;
  }

  /**
   * Appends a value to the file and return it's offset.
   * @param p write position
   * @param v byte array to be appended
   */
  public void writeToken(final long p, final byte[] v) {
    cursor(p);
    writeToken(v, 0, v.length);
  }

  /**
   * Write a token to the file.
   * @param buf buffer containing the token
   * @param offset offset in the buffer where the token starts
   * @param length token length
   */
  void writeToken(final byte[] buf, final int offset, final int length) {
    writeNum(length);

    final int last = offset + length;
    int o = offset;

    while(o < last) {
      final int l = Math.min(last - o, IO.BLOCKSIZE - off);
      buffer(off == IO.BLOCKSIZE).copyFrom(off, buf, o, l);
      off += l;
      o += l;
    }

    // adjust file size if needed
    final long nl = file.currentBuffer().getPos() + off;
    if(nl > len) length(nl);
  }

  /**
   * Appends a value to the file and return it's offset.
   * @param v number to be appended
   */
  private void writeNum(final int v) {
    if(v < 0 || v > 0x3FFFFFFF) {
      write(0xC0); write(v >>> 24); write(v >>> 16); write(v >>> 8); write(v);
    } else if(v > 0x3FFF) {
      write(v >>> 24 | 0x80); write(v >>> 16);
      write(v >>> 8); write(v);
    } else if(v > 0x3F) {
      write(v >>> 8 | 0x40); write(v);
    } else {
      write(v);
    }
  }

  /**
   * Returns the offset to a free slot for writing an entry with the
   * specified length. Fills the original space with 0xFF to facilitate
   * future write operations.
   * @param pos original offset
   * @param size size of new text entry
   * @return new offset to store text
   */
  public long free(final long pos, final int size) {
    // old text size (available space)
    int os = readNum(pos) + (int) (cursor() - pos);

    // extend available space by subsequent zero-bytes
    cursor(pos + os);
    for(; pos + os < len && os < size && read() == 0xFF; os++);

    long o = pos;
    if(pos + os == len) {
      // entry is placed last: reset file length (discard last entry)
      length(pos);
    } else {
      int t = size;
      if(os < size) {
        // gap is too small for new entry...
        // reset cursor to overwrite entry with zero-bytes
        cursor(pos);
        t = 0;
        // place new entry after last entry
        o = len;
      } else {
        // gap is large enough: set cursor to overwrite remaining bytes
        cursor(pos + size);
      }
      // fill gap with 0xFF for future updates
      while(t++ < os) write(0xFF);
    }
    return o;
  }

  // PRIVATE METHODS ==========================================================

  /**
   * Returns the current or next buffer.
   * @param next next block
   * @return buffer
   */
  private Buffer buffer(final boolean next) {
    if(next) cursor(file.currentBuffer().getPos() + IO.BLOCKSIZE);
    return file.currentBuffer();
  }
}
