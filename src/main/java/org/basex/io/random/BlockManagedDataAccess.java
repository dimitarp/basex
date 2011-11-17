package org.basex.io.random;

import static java.lang.Integer.*;
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
  /** Power of the number of blocks per segment. */
  public static final int SEGMENTBLOCKPOWER = IO.BLOCKPOWER + 3;
  /** Number of blocks per segment. */
  public static final long SEGMENTBLOCKS = 1L << SEGMENTBLOCKPOWER;
  /** Number of bytes per segment. */
  public static final long SEGMENTSIZE = SEGMENTBLOCKS << IO.BLOCKPOWER;
  /** Bit mask for a word in the free blocks bit map. */
  public static final int BITMASK = 0xFF;
  /** Number of blocks (both header and data) allocated in the file. */
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

  @Override
  public long cursor() {
    final long block = buffer(false).pos >>> IO.BLOCKPOWER;
    final long segments = segments(block + 1L);
    final long logicalBlock = block - segments;
    return position(logicalBlock) + off;
  }

  @Override
  public void cursor(final long l) {
    gotoBlock(l >>> IO.BLOCKPOWER);
    off = (int) modulo2(l, IO.BLOCKSIZE);
  }

  @Override
  public long length() {
    final long len = super.length();
    final long lastBlock = (len - 1) >>> IO.BLOCKPOWER;
    final long segments = segments(lastBlock + 1L);
    final long logicalBlock = lastBlock - segments;
    return position(logicalBlock) + modulo2(len, IO.BLOCKSIZE) + 1;
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
    final long headers = segments(blocks);

    // check headers for a free data block
    for(long h = 0L; h < headers; ++h) {
      final long header = headerBlock(h);
      final int b = getFree(position(header));
      if(b >= 0) {
        final long block = b + (h << SEGMENTBLOCKPOWER);
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

    super.cursor(wordPos);
    final int word = super.read();

    super.cursor(wordPos);
    super.write(clear(word, n));
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
      final int word = super.read();
      if(word != BITMASK) {
        final int free = numberOfTrailingZeros(~word);
        super.cursor(pos + p);
        super.write(set(word, free));
        return free + (p << 3);
      }
    }
    // no free blocks
    return -1;
  }

  // static methods
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
