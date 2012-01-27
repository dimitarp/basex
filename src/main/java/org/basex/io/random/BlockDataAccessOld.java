package org.basex.io.random;

import java.io.File;
import java.io.IOException;
import org.basex.io.IO;

/**
 * Access files block-wise.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
@Deprecated
public class BlockDataAccessOld extends DataAccess {
  /** Power of 2 of the size of a header reference in bytes. */
  private static final short BLOCKREFPOWER = 2;
  /** Size of a block reference in bytes. */
  private static final short BLOCKREFSIZE = 4;
  /** Number of block references in a header block (default: 3968 bytes). */
  private static final int BLOCKREFS = 992;
  /** Size of the bit map tracking free blocks (default: 992 bits). */
  private static final short FREEMAPSIZE = BLOCKREFS >>> 3;
  /** Offset in a header block, where the list of block references starts. */
  private static final int BLOCKLISTOFF = BLOCKREFSIZE + FREEMAPSIZE;
  /** Invalid reference value. */
  private static final long NIL = 0L;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException I/O Exception
   */
  public BlockDataAccessOld(final File f) throws IOException {
    super(f);
  }

  /**
   * Sets the disk cursor to the beginning of the specified block.
   * @param n logical block number
   */
  public void gotoBlock(final int n) {
    final long pos = header(n);
    // number of the required block within the header block
    final int blockNum = n % BLOCKREFS;
    if(isFree(pos, blockNum))
      throw new RuntimeException("Empty requested block: " + n);

    // offset within the header block, where the required block ref is
    final int headerOff = BLOCKLISTOFF + (blockNum << BLOCKREFPOWER);
    // read the required block reference and position cursor at its beginning
    cursor(read4(pos + headerOff) << IO.BLOCKPOWER);
  }

  /**
   * Delete a block.
   * @param n number of the block
   */
  public void deleteBlock(final int n) {
    final long pos = header(n);
    // number of the required block within the header block
    final int blockNum = n % BLOCKREFS;
    setFree(pos, blockNum);
  }

  /**
   * Get a new free block.
   * @return number of the free block
   */
  public int createBlock() {
    // TODO
    // search header blocks for free blocks
    long header = 0L;
    int blockNum = -1;
    do {
      blockNum = getFree(header);
      if(blockNum >= 0) {
        final int headerOff = BLOCKLISTOFF + (blockNum << BLOCKREFPOWER);
        final int blockRef = read4(headerOff);
        if(blockRef != NIL) return blockRef;
      }
      header = read4(header);
    } while(header != NIL);
    // no free blocks -> create new header block
    return 0;
  }

  // private methods

  /**
   * Position the cursor at the beginning of the header block which contains the
   * reference to the required block.
   * @param n required block number
   * @return physical position of the header block
   */
  private long header(final int n) {
    // number of header block, which has the required block ref
    final int headerNum = n / BLOCKREFS;
    // position the cursor at the beginning of the headerNum'th header block
    long pos = 0L;
    for(int i = 0; i <= headerNum; ++i) {
      pos = read4(pos);
      if(pos == NIL) throw new RuntimeException("Could not find block: " + n);
      pos <<= IO.BLOCKPOWER;
    }
    return pos;
  }

  /**
   * Check if the n<sup>th</sup> block in the header block at the given position
   * is free.
   * @param pos header block physical position
   * @param n block number within the header block
   * @return {@code true} if the block is free
   */
  private boolean isFree(final long pos, final int n) {
    return (readFreeMapWord(pos, n) & ((byte) 1 << n)) != 0;
  }

  /**
   * Mark the n<sup>th</sup> block in the header block at the given position as
   * free.
   * @param pos header block physical position
   * @param n block number within the header block
   */
  private void setFree(final long pos, final int n) {
    write1(readFreeMapWord(pos, n) & ~((byte) 1 << n));
  }

  private int getFree(final long pos) {
    return -1;
  }

  /**
   * Read the word from the free block bit map of the header block at the given
   * position, where the corresponding bit of n<sup>th</sup> block is stored.
   * @param pos header block physical position
   * @param n block number within the header block
   * @return bit map word
   */
  private byte readFreeMapWord(final long pos, final int n) {
    return read1(pos + BLOCKREFSIZE + (n >>> 3));
  }
}
