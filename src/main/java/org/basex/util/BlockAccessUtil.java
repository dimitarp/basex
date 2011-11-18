package org.basex.util;

import org.basex.io.IO;

/**
 * Utility methods for accessing blocks.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public final class BlockAccessUtil {
  /** Power of the number of blocks per segment. */
  public static final int SEGMENTBLOCKPOWER = IO.BLOCKPOWER + 3;
  /** Number of blocks per segment. */
  public static final long SEGMENTBLOCKS = 1L << SEGMENTBLOCKPOWER;
  /** Number of bytes per segment. */
  public static final long SEGMENTSIZE = SEGMENTBLOCKS << IO.BLOCKPOWER;
  /** Bit mask for a word in the free blocks bit map. */
  public static final int BITMASK = 0xFF;

  /** Private constructor. */
  private BlockAccessUtil() { }

  /**
   * Calculate the number of header blocks of a file with the specified total
   * number of blocks (both header and data).
   * @param blocks number of blocks
   * @return number of header blocks
   */
  public static long segments(final long blocks) {
    // each segment has SEGMENTBLOCKS + 1 blocks
    return divRoundUp(blocks, SEGMENTBLOCKS + 1L);
  }

  /**
   * Positive integer division, rounding the result up.
   * @param x dividend
   * @param y divisor
   * @return result rounded to the next bigger or equal integer
   */
  public static long divRoundUp(final long x, final long y) {
    final long d = x / y;
    return x % y == 0L ? d : d + 1L;
  }

  /**
   * Calculate the number of blocks (both header and data) of a file with the
   * specified length.
   * @param len file length
   * @return number of blocks
   */
  public static long blocks(final long len) {
    // same as divRoundUp, but uses bit-shift for division
    final long d = len >>> IO.BLOCKPOWER;
    return modulo2(len, IO.BLOCKSIZE) == 0L ? d : d + 1L;
  }

  /**
   * Calculate the actual block number in the file.
   * @param n logical data block number
   * @return physical block number
   */
  public static long dataBlock(final long n) {
    final long segment = n >>> SEGMENTBLOCKPOWER;
    final long block = segment + n + 1L;
    return block;
  }

  /**
   * Calculate the actual block number for the given header block.
   * @param n logical header block number
   * @return physical block number
   */
  public static long headerBlock(final long n) {
    return (n << SEGMENTBLOCKPOWER) + n;
  }

  /**
   * Calculate the actual header block number for the given data block.
   * @param n logical data block number
   * @return physical block number of the header block
   */
  public static long headerBlockFor(final long n) {
    final long segment = n >>> SEGMENTBLOCKPOWER;
    return headerBlock(segment);
  }

  /**
   * Calculate the block number for the given data block relative to the
   * corresponding header block.
   * @param n logical data block number
   * @return block number relative to the header block
   */
  public static long dataBlockOffset(final long n) {
    return modulo2(n, SEGMENTBLOCKS);
  }

  /**
   * Calculate logical position of a corresponding physical position.
   * @param pos physical position
   * @return logical position
   */
  public static long logicalPosition(final long pos) {
    final long physicalBlock = pos >>> IO.BLOCKPOWER;
    final long segments = segments(physicalBlock + 1L);
    final long logicalBlock = physicalBlock - segments;
    return position(logicalBlock) + modulo2(pos, IO.BLOCKSIZE);
  }

  /**
   * Calculate the start position of a block.
   * @param n block number
   * @return position in the file
   */
  public static long position(final long n) {
    return n << IO.BLOCKPOWER;
  }

  /**
   * Calculate x % y, where y = 2<sup>n</sup>.
   * @param x dividend
   * @param y divisor
   * @return modulo
   */
  public static long modulo2(final long x, final long y) {
    return x & (y - 1L);
  }

  /**
   * Clear a bit in word.
   * @param word word
   * @param n bit number; if bigger than word size, rotation will be performed
   * @return word with the cleared bit
   */
  public static int clear(final int word, final long n) {
    return word & ~(1 << modulo2(n, Byte.SIZE));
  }

  /**
   * Set a bit in word.
   * @param word word
   * @param n bit number; if bigger than word size, rotation will be performed
   * @return word with the set bit
   */
  public static int set(final int word, final long n) {
    return word | (1 << modulo2(n, Byte.SIZE));
  }
}
