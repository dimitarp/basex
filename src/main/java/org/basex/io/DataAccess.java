package org.basex.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.basex.util.Num;
import org.basex.util.Util;

/**
 * This class allows positional read and write access to a database file.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class DataAccess {
  /** Buffer manager. */
  private final Buffers bm = new Buffers();
  /** Reference to the data input stream. */
  private final RandomAccessFile file;
  /** File length. */
  private long len;
  /** Changed flag. */
  private boolean changed;
  /** Offset. */
  private int off;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException IO Exception
   */
  public DataAccess(final File f) throws IOException {
    file = new RandomAccessFile(f, "rw");
    len = file.length();
    cursor(0);
  }

  /**
   * Flushes the buffered data.
   * @throws IOException I/O exception
   */
  public synchronized void flush() throws IOException {
    for(final Buffer b : bm.all()) if(b.dirty) writeBlock(b);
    if(changed) file.setLength(len);
  }

  /**
   * Closes the data access.
   * @throws IOException I/O exception
   */
  public synchronized void close() throws IOException {
    flush();
    // if(changed) file.setLength(len);
    file.close();
  }

  /**
   * Returns the current file position.
   * @return position in the file
   */
  public long pos() {
    return buffer(false).pos + off;
  }

  /**
   * Sets the file length.
   * @param l file length
   */
  public void length(final long l) {
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
    return pos() < len;
  }

  /**
   * Reads a byte value.
   * @return integer value
   */
  public synchronized byte read1() {
    return (byte) read();
  }

  /**
   * Reads an integer value.
   * @return integer value
   */
  public synchronized int read4() {
    return (read() << 24) + (read() << 16) + (read() << 8) + read();
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
    int l = readNum();
    int ll = IO.BLOCKSIZE - off;
    final byte[] b = new byte[l];

    System.arraycopy(buffer(false).data, off, b, 0, Math.min(l, ll));
    if(l > ll) {
      l -= ll;
      while(l > IO.BLOCKSIZE) {
        System.arraycopy(buffer(true).data, 0, b, ll, IO.BLOCKSIZE);
        ll += IO.BLOCKSIZE;
        l -= IO.BLOCKSIZE;
      }
      System.arraycopy(buffer(true).data, 0, b, ll, l);
    }
    off += l;
    return b;
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
   * @param l length
   * @return byte array
   */
  public synchronized byte[] readBytes(final int l) {
    final byte[] b = new byte[l];
    for(int i = 0; i < b.length; ++i) b[i] = (byte) read();
    return b;
  }

  /**
   * Sets the disk cursor.
   * @param p read position
   */
  public void cursor(final long p) {
    off = (int) (p & IO.BLOCKSIZE - 1);
    final long b = p - off;
    if(!bm.cursor(b)) return;

    final Buffer bf = bm.current();
    try {
      if(bf.dirty) writeBlock(bf);
      bf.pos = b;
      file.seek(bf.pos);
      if(bf.pos < file.length())
        file.readFully(bf.data, 0, (int) Math.min(len - bf.pos, IO.BLOCKSIZE));
    } catch(final IOException ex) {
      Util.stack(ex);
    }
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
   * Appends a value to the file and return it's offset.
   * @param p write position
   * @param v byte array to be appended
   */
  public void writeBytes(final long p, final byte[] v) {
    cursor(p);
    writeNum(v.length);
    for(final byte b : v) write(b);
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
   * Writes an integer value to the specified output stream.
   * @param p write position
   * @param v byte array to be appended
   */
  public void writeInt(final long p, final int v) {
    cursor(p);
    writeInt(v);
  }

  /**
   * Writes an integer value to the specified output stream.
   * @param v value to be written
   */
  public void writeInt(final int v) {
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
   * Appends values to the file.
   * @param p write position
   * @param v byte array to be appended
   */
  public void writeNums(final long p, final int[] v) {
    cursor(p);
    writeNum(v.length);
    for(final int n : v) writeNum(n);
  }

  // PRIVATE METHODS ==========================================================

  /**
   * Writes the specified block to disk.
   * @param bf buffer to write
   * @throws IOException I/O exception
   */
  private void writeBlock(final Buffer bf) throws IOException {
    file.seek(bf.pos);
    file.write(bf.data);
    bf.dirty = false;
  }

  /**
   * Appends a value to the file and return it's offset.
   * @param v number to be appended
   */
  public void writeNum(final int v) {
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
   * Reads the next byte.
   * @return next byte
   */
  private int read() {
    final Buffer bf = buffer(off == IO.BLOCKSIZE);
    return bf.data[off++] & 0xFF;
  }

  /**
   * Writes the next byte.
   * @param b byte to be written
   */
  private void write(final int b) {
    final Buffer bf = buffer(off == IO.BLOCKSIZE);
    bf.data[off++] = (byte) b;
    length(Math.max(len, bf.pos + off));
    bf.dirty = true;
  }

  /**
   * Returns the current or next buffer.
   * @param next next block
   * @return buffer
   */
  private Buffer buffer(final boolean next) {
    if(next) {
      off = 0;
      cursor(bm.current().pos + IO.BLOCKSIZE);
    }
    return bm.current();
  }
}
