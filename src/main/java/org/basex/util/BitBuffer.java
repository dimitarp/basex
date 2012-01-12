package org.basex.util;

import static org.basex.util.BlockAccessUtil.*;

/**
 * Buffer which allows bit-wise access to the data.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public final class BitBuffer {
  /** Power of the size of a word in bits. */
  private static final int WORD_POWER = 6;
  /** Bit storage. */
  private final long[] words;

  /**
   * Constructor.
   * @param size maximal number of bits to be stored
   */
  public BitBuffer(final long size) {
    words = new long[(int) divRoundUp(size, Long.SIZE)];
  }

  /**
   * Initialize the buffer with the given data.
   * @param data data
   */
  public void init(final byte[] data) {
    for(int i = 0, j = 0; i < words.length; ++i, j += 8) {
      words[i] = (data[j] & 0xFFL) |
          ((data[j + 1] & 0xFFL) << 0x08) |
          ((data[j + 2] & 0xFFL) << 0x10) |
          ((data[j + 3] & 0xFFL) << 0x18) |
          ((data[j + 4] & 0xFFL) << 0x20) |
          ((data[j + 5] & 0xFFL) << 0x28) |
          ((data[j + 6] & 0xFFL) << 0x30) |
          ((data[j + 7] & 0xFFL) << 0x38);
    }
  }

  /**
   * Read a given number of bits starting from the specified position.
   * @param pos start position
   * @param num number of bits (max 64)
   * @return a long value containing the read bits
   */
  public long read(final long pos, final int num) {
    final int idx = (int) (pos >>> WORD_POWER);
    final long wordEnd = (idx + 1) << WORD_POWER;
    final long end = pos + num;

    if(end > wordEnd) {
      // the bits span two words
      final int lowNum = (int) (wordEnd - pos);
      final int highNum = (int) (end - wordEnd);
      return (read(idx + 1, 0, highNum) << lowNum) | read(idx, pos, lowNum);
    }
    return read(idx, pos, num);
  }

  /**
   * Write the specified bits at the given position.
   * @param pos write position
   * @param num number of bits to write
   * @param bits bits to write
   */
  public void write(final long pos, final int num, final long bits) {
    // TODO
  }

  /**
   * Serialize the current buffer to the given byte array.
   * @param data byte array
   */
  public void serialize(final byte[] data) {
    // TODO
  }

  /**
   * Read a given number of bits from the word with the given index starting
   * from the specified position.
   * @param idx word index
   * @param pos start position
   * @param num number of bits; should not exceed the word boundary
   * @return a long value containing the read bits
   */
  private long read(final int idx, final long pos, final int num) {
    return (words[idx] & mask(pos, num)) >>> pos;
  }

  /**
   * Create a bit-mask where only a given number of bits at a give position are
   * set.
   * @param pos start position; if bigger than 64, then {@code pos % 64} will be
   * used
   * @param num number of set bits
   * @return bit-mask
   */
  public static long mask(final long pos, final int num) {
    return ((2L << (num - 1)) - 1L) << pos;
  }
}
