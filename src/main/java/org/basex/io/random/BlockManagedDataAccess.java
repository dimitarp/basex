package org.basex.io.random;

import static java.lang.Math.*;
import java.io.File;
import java.io.IOException;
import org.basex.io.IO;

/**
 * Access files block-wise.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockManagedDataAccess extends DataAccess {
  /** Power of the number of blocks per header block. */
  public static final int BLOCKSPERHEADERPOWER = IO.BLOCKPOWER + 3;
  /** Number of blocks per header block. */
  public static final long BLOCKSPERHEADER = 1 << BLOCKSPERHEADERPOWER;
  /** Bit mask for a word in the free blocks bit map. */
  public static final byte BITMASK = (byte) 0xFF;
  /** Number of blocks allocated in the file. */
  private long blocks;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException I/O Exception
   */
  public BlockManagedDataAccess(final File f) throws IOException {
    super(f);
    blocks = blocks(super.length());
  }

  /**
   * Sets the disk cursor to the beginning of the specified block.
   * @param n logical block number
   */
  public void gotoBlock(final long n) {
    super.cursor(position(dataBlock(n)));
  }

  /**
   * Delete a block.
   * @param n logical block number
   */
  public void deleteBlock(final long n) {
    setFree(position(headerBlockFor(n)), dataBlockOffset(n));
  }

  /**
   * Get a new free block.
   * @return logical number of the free block
   */
  public long createBlock() {
    final long headers = divRoundUp(blocks, BLOCKSPERHEADER + 1L);

    // check headers for a free data block
    for(long h = 0L; h < headers; ++h) {
      final long header = headerBlock(h);
      final int b = getFree(position(header));
      if(b >= 0) {
        final long block = b + (h << BLOCKSPERHEADERPOWER);
        if(block + headers >= blocks) ++blocks;
        return block;
      }
    }

    // no free blocks: create new header block + new data block
    initHeader(position(blocks));

    // logical number of the new block will be:
    // block = (blocks + 2) - (headers + 1) - 1 = blocks - headers
    final long block = blocks - headers;
    blocks += 2L;
    return block;
  }

  // private methods
  /**
   * Initialize a header block at the specified position and mark its first data
   * block as occupied.
   * @param pos header block physical position
   */
  private void initHeader(final long pos) {
    super.cursor(pos);
    // mark the first data block as occupied
    super.write(1);
  }

  /**
   * Mark the n<sup>th</sup> block in the header block at the given position as
   * free.
   * @param pos header block physical position
   * @param n block number within the header block
   */
  private void setFree(final long pos, final long n) {
    final long wordPos = pos + (n >>> 3);
    final byte word = super.read1(wordPos);
    super.cursor(wordPos);
    super.write1(word & ~((byte) (1 << n))); // TODO: wrong!
  }

  /**
   * Find a free block in the header block at the given position, and mark it as
   * occupied.
   * @param pos header block physical position
   * @return block number relative to the header block; {@code -1} if all blocks
   *         are marked as occupied in the header
   */
  private int getFree(final long pos) {
    // find a 0 bit in the block at pos and set it to 1.
    super.cursor(pos);
    for(int p = 0; p < IO.BLOCKSIZE; ++p) {
      final byte b = super.read1();
      if(b != BITMASK) {
        final int free = Long.numberOfTrailingZeros(~(long) b);
        super.cursor(pos + p);
        super.write1(b | ((byte) (1 << free)));
        return free + (p << 3);
      }
    }
    return -1;
  }

  // static methods
  /**
   * Positive integer division, rounding the result up.
   * @param x dividend
   * @param y divisor
   * @return result rounded to the next bigger or equal integer
   */
  public static long divRoundUp(final long x, final long y) {
    final long sum = x + y;
    if(sum > 0) return (sum - 1) / y;
    return Double.valueOf(ceil(x / (double) y)).longValue();
  }

  /**
   * Calculate the number of blocks of a file with the specified length.
   * @param len file length
   * @return number of blocks
   */
  public static long blocks(final long len) {
    // same as divRoundUp, but uses bit-shift for division
    final long sum = len + IO.BLOCKSIZE;
    if(sum > 0) return (sum - 1) >>> IO.BLOCKPOWER;
    return Double.valueOf(ceil(len / (double) IO.BLOCKSIZE)).longValue();
  }

  /**
   * Calculate the actual block number in the file.
   * @param n logical data block number
   * @return physical block number
   */
  public static long dataBlock(final long n) {
    final long segment = n >>> BLOCKSPERHEADERPOWER;
    final long block = segment + n + 1;
    return block;
  }

  /**
   * Calculate the actual block number for the given header block.
   * @param n logical header block number
   * @return physical block number
   */
  public static long headerBlock(final long n) {
    return (n << BLOCKSPERHEADERPOWER) + n;
  }

  /**
   * Calculate the actual header block number for the given data block.
   * @param n logical data block number
   * @return physical block number of the header block
   */
  public static long headerBlockFor(final long n) {
    final long segment = n >>> BLOCKSPERHEADERPOWER;
    return headerBlock(segment);
  }

  /**
   * Calculate the block number for the given data block relative to the
   * corresponding header block.
   * @param n logical data block number
   * @return block number relative to the header block
   */
  public static long dataBlockOffset(final long n) {
    return n & ((1 << BLOCKSPERHEADERPOWER) - 1);
  }

  /**
   * Calculate the start position of a block.
   * @param n block number
   * @return position in the file
   */
  public static long position(final long n) {
    return n << IO.BLOCKPOWER;
  }
}
