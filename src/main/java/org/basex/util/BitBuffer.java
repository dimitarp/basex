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
    init(data, 0, data.length);
  }

  /**
   * Initialize the buffer with the given data.
   * @param data data
   * @param start start offset in the data array
   * @param end end + 1 element in the data array
   */
  public void init(final byte[] data, final int start, final int end) {
    final int last = end - ((end - start) & 7);
    int i = 0, j = start;
    for(; j < last && i < words.length; ++i, j += 8) {
      words[i] =
          ((data[j] & 0xFFL)) |
          ((data[j + 1] & 0xFFL) << 0x08) |
          ((data[j + 2] & 0xFFL) << 0x10) |
          ((data[j + 3] & 0xFFL) << 0x18) |
          ((data[j + 4] & 0xFFL) << 0x20) |
          ((data[j + 5] & 0xFFL) << 0x28) |
          ((data[j + 6] & 0xFFL) << 0x30) |
          ((data[j + 7] & 0xFFL) << 0x38);
    }

    for(int n = 0; j < end; ++j, n += 8) words[i] |= (data[j] & 0xFFL) << n;
  }

  /**
   * Serialize the current buffer to the given byte array.
   * @param data byte array
   */
  public void serialize(final byte[] data) {
    serialize(data, 0, data.length);
  }

  /**
   * Serialize the current buffer to the given byte array.
   * @param data byte array
   * @param start start offset in the data array
   * @param end end + 1 element in the data array
   */
  public void serialize(final byte[] data, final int start, final int end) {
    final int last = end - ((end - start) & 7);
    int i = 0, j = start;
    for(; j < last && i < words.length; ++i, j += 8) {
      data[j] = (byte) (words[i]);
      data[j + 1] = (byte) (words[i] >>> 0x08);
      data[j + 2] = (byte) (words[i] >>> 0x10);
      data[j + 3] = (byte) (words[i] >>> 0x18);
      data[j + 4] = (byte) (words[i] >>> 0x20);
      data[j + 5] = (byte) (words[i] >>> 0x28);
      data[j + 6] = (byte) (words[i] >>> 0x30);
      data[j + 7] = (byte) (words[i] >>> 0x38);
    }

    for(int n = 0; j < end; ++j, n += 8) data[j] = (byte) (words[i] >> n);
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
    final int idx = (int) (pos >>> WORD_POWER);
    final long wordEnd = (idx + 1) << WORD_POWER;
    final long end = pos + num;

    if(end > wordEnd) {
      // the bits span two words
      final int lowNum = (int) (wordEnd - pos);
      final int highNum = (int) (end - wordEnd);
      write(idx + 1, 0, highNum, bits >>> lowNum);
      write(idx, pos, lowNum, bits);
    } else {
      write(idx, pos, num, bits);
    }
  }

  /**
   * Read a given number of bits from the word with the given index starting
   * from the specified position.
   * @param idx word index
   * @param pos start position
   * @param num number of bits; should not exceed the word boundary
   * @return a long value containing the read bits at the beginning
   */
  private long read(final int idx, final long pos, final int num) {
    return (words[idx] & mask(pos, num)) >>> pos;
  }

  /**
   * Write a given number of bits to the word with the given index starting from
   * the specified position.
   * @param w word index
   * @param pos start position
   * @param num number of bist; should not exceed the word boundary
   * @param bits a long value containing the write bits at the beginning
   */
  private void write(final int w, final long pos, final int num,
      final long bits) {
    words[w] = (words[w] & ~mask(pos, num)) | ((bits & mask(0, num)) << pos);
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
