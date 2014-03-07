package org.basex.io.random.file;

import org.basex.io.IO;

/**
 * This class provides a simple, clock-based buffer management.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
final class Buffers {
  /** Number of buffers (must be 1 << n). */
  private static final int BUFFERS = 1 << 4;
  /** Buffers. */
  private final Buffer[] buf = new Buffer[BUFFERS];
  /** Current buffer offset. */
  private int off;

  /**
   * Constructor.
   */
  Buffers() {
    for(int b = 0; b < BUFFERS; ++b) buf[b] = new BlockBuffer(IO.BLOCKSIZE);
  }

  /**
   * Returns all buffers.
   * @return buffers
   */
  Buffer[] all() {
    return buf;
  }

  /**
   * Returns the current buffer.
   * @return current buffer
   */
  Buffer current() {
    return buf[off];
  }

  /**
   * Chooses a buffer and sets the offset.
   * @param p buffer pointer
   * @return true if cursor has changed
   */
  boolean cursor(final long p) {
    final int o = off;
    do {
      if(buf[off].getPos() == p) return false;
    } while((off = off + 1 & BUFFERS - 1) != o);
    off = o + 1 & BUFFERS - 1;
    return true;
  }
}
