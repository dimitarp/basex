package org.basex.io.random;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.basex.io.IO;
import org.basex.util.BitArray;

/**
 * Access files blockwise.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockDataAccess extends DataAccess {
  /** Power of 2 of the size of a page reference in bytes. */
  private static final short BLOCKREFPOWER = 2;
  /** Size of a page reference in bytes. */
  private static final short BLOCKREFSIZE = 4;
  /** Number of page references in a directory page (default: 3968 bytes). */
  private static final int BLOCKREFS = 992;
  /** Size of the bit map tracking free blocks (default: 992 bits). */
  private static final short FREEMAPSIZE = BLOCKREFS >>> 3;
  /** Offset in a directory block, where the list of block references starts. */
  private static final int BLOCKLISTOFF = BLOCKREFSIZE + FREEMAPSIZE;
  /** Invalid reference value. */
  private static final long NIL = 0L;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException I/O Exception
   */
  public BlockDataAccess(final File f) throws IOException {
    super(f);
  }

  /**
   * Sets the disk cursor to the beginning of the specified block.
   * @param n block number
   */
  public void gotoBlock(final int n) {
    // number of the block in the directory, which has the required block ref
    final int dirBlockNum = n / BLOCKREFS;
    // offset within the directory block, where the required block ref is
    final int dirBlockOff = n % BLOCKREFS + BLOCKLISTOFF << BLOCKREFPOWER;

    // position cursor at the beginning of the dirBlockNum'th directory block
    long pos = 0L;
    for(long i = 0; i <= dirBlockNum; ++i) {
      pos = read4(pos);
      if(pos == NIL)
        throw new RuntimeException("Could not find block with number " + n);
      pos <<= IO.BLOCKPOWER;
    }

    // read the required block reference and position cursor at its beginning
    final int ref = read4(pos + dirBlockOff);

    cursor(ref << IO.BLOCKPOWER);
  }

  /**
   * Get a new free block.
   * @return number of the free block
   */
  public int createBlock() {
    // TODO
    return 0;
  }

  /**
   * Delete a block.
   * @param n number of the block
   */
  public void deleteBlock(final int n) {
    // TODO
  }

  private BitArray readFreeMap() {
    // TODO
    return null;
  }

  private boolean isFree(final int ref) {
    // TODO
    return false;
  }
}
