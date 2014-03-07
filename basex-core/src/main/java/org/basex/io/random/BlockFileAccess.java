package org.basex.io.random;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.basex.io.IOFile;
import org.basex.util.Util;

/**
 * Block-wise file access.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
abstract class BlockFileAccess {
  /** Buffer manager. */
  private final Buffers bm = new Buffers();

  /**
   * Open a file and create a new {@link BlockFileAccess}.
   * @param fl file to open
   * @return block-wise file access
   * @throws FileNotFoundException if given file does not exist
   */
  public static BlockFileAccess openRandomAccess(final IOFile fl) throws FileNotFoundException {
    RandomAccessFile file = new RandomAccessFile(fl.file(), "rw");
    return new RandomAccessFileBlockFileAccess(file);
  }

  /**
   * Open a file and create a new {@link BlockFileAccess}.
   * @param fl file to open
   * @return block-wise file access
   * @throws IOException if file cannot be accessed
   */
  public static BlockFileAccess open(final IOFile fl) throws IOException {
    FileChannel file = new RandomAccessFile(fl.file(), "rw").getChannel();
    //FileChannel file = FileChannel.open(fl.file().toPath(), READ, WRITE);
    return new FileChannelBlockFileAccess(file);
  }

  /**
   * Close the file handle.
   * @throws IOException I/O exception
   */
  public abstract void close() throws IOException;

  /**
   * Write the given buffer to the file.
   * @param b buffer to write
   * @throws IOException I/O exception
   */
  public abstract void write(Buffer b) throws IOException;

  /**
   * Read a buffer from the file.
   * @param b buffer to read
   * @param max maximal bytes to read
   * @throws IOException I/O exception
   */
  public abstract void read(Buffer b, int max) throws IOException;

  /**
   * Read a buffer from the file.
   * @param b buffer to read
   * @throws IOException I/O exception
   */
  public abstract void read(Buffer b) throws IOException;

  /**
   * Get the length of the file.
   * @return length of the file in bytes
   * @throws IOException I/O exception
   */
  public abstract long length() throws IOException;

  /**
   * Set the new length of the file.
   * @param l new length to set
   * @throws IOException I/O exception
   */
  public abstract void setLength(long l) throws IOException;

  /**
   * Get the underlying file channel.
   * @return file channel
   */
  public abstract FileChannel getChannel();

  /**
   * Flush dirty buffers to the file.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    for(final Buffer b : bm.all()) if(b.isDirty()) write(b);
  }

  /**
   * Read the block at the given position.
   * @param blockPosition block position
   * @return buffer with the data from the block
   */
  public Buffer readBlock(long blockPosition) {
    final boolean changed = bm.cursor(blockPosition);
    final Buffer bf = bm.current();
    if(changed) {
      try {
        if(bf.isDirty()) write(bf);
        bf.setPos(blockPosition);
        read(bf);
      } catch(final IOException ex) {
        Util.stack(ex);
      }
    }
    return bf;
  }

  /**
   * Get the current buffer.
   * @return current buffer
   */
  public Buffer currentBuffer() {
    return bm.current();
  }
}
