package org.basex.io.random;

import static java.lang.Integer.*;
import static org.basex.util.BlockAccessUtil.*;
import java.io.IOException;
import org.basex.io.*;

/**
 * Access files block-wise.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockDataAccess extends DataAccess {
  /** Number of blocks (both header and data) allocated in the file. */
  private long blocks;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException I/O Exception
   */
  public BlockDataAccess(final IOFile f) throws IOException {
    super(f);
    blocks = blocks(super.length());
  }

  @Override
  public long cursor() {
    return logicalPosition(super.cursor());
  }

  @Override
  public void cursor(final long l) {
    super.cursor(physicalPosition(l));
  }

  @Override
  public long length() {
    final long len = super.length();
    return len < IO.BLOCKSIZE ? 0 : logicalPosition(super.length());
  }

  @Override
  public boolean more() {
    return cursor() < length();
  }

  @Override
  protected long blockPos() {
    return logicalPosition(super.blockPos());
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
}
