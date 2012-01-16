package org.basex.test.util;

import static org.junit.Assert.*;
import static org.basex.util.BitBuffer.*;
import java.lang.reflect.Field;
import org.basex.util.BitBuffer;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for methods of class {@link BitBuffer}.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public final class BitBufferTest {
  /** Test data. */
  private static final byte[] DATA = new byte[] {
    (byte) 0x00, (byte) 0xFF, (byte) 0x00, (byte) 0x00,
    (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF,

    (byte) 0xFF, (byte) 0xF0, (byte) 0x00, (byte) 0x00,
    (byte) 0x00, (byte) 0xFF, (byte) 0x00, (byte) 0x00
  };

  /** Test data as long. */
  private static final long[] DATALONG = new long[] {
    0xFF0000000000FF00L,
    0x0000FF000000F0FFL
  };

  /** Subject under test. */
  private BitBuffer sut;

  /** Set up method. */
  @Before
  public void setUp() {
    sut = new BitBuffer(DATA.length * 8);
    setWords(DATALONG);
  }

  /** Test for {@link BitBuffer#mask(long, int)}. */
  @Test
  public void testMask() {
    // boundary case: all bits are set
    assertEquals(0xFFFFFFFFFFFFFFFFL, mask(0, 64));
    // the first 20 bits are set
    assertEquals(0x00000000000FFFFFL, mask(0, 20));
    // the 20 bits, starting from bit 16 are set
    assertEquals(0x0000000FFFFF0000L, mask(16, 20));
    // the last 20 bits are set
    assertEquals(0xFFFFF00000000000L, mask(44, 20));

    // same as above, but the position is > 64
    assertEquals(0xFFFFFFFFFFFFFFFFL, mask(128 + 0, 64));
    assertEquals(0x00000000000FFFFFL, mask(128 + 0, 20));
    assertEquals(0x0000000FFFFF0000L, mask(128 + 16, 20));
    assertEquals(0xFFFFF00000000000L, mask(128 + 44, 20));
  }

  /** Test for {@link BitBuffer#init(byte[])}. */
  @Test
  public void testInit() {
    setWords(new long[DATALONG.length]);
    sut.init(DATA);
    assertWords(DATALONG);
  }

  /** Test for {@link BitBuffer#serialize(byte[])}. */
  @Test
  public void testSerialize() {
    final byte[] data = new byte[DATA.length];
    sut.serialize(data);
    assertArrayEquals(DATA, data);
  }

  /** Test for {@link BitBuffer#read(long, int)}. */
  @Test
  public void testRead() {
    assertEquals(0xFFL, sut.read(8, 8));
    assertEquals(0xFFFFL, sut.read(56, 16));
    assertEquals(0x00FFFFL, sut.read(56, 20));
    assertEquals(0xF0FFFFL, sut.read(56, 24));
    assertEquals(0xFF000000F0FFFFL, sut.read(56, 56));
  }

  /** Test for {@link BitBuffer#read(long, int)}. */
  @Test
  public void testWrite() {
    setWords(DATALONG);
    sut.write(4L, 8, 0x0FL);
    assertWords(0xFF0000000000F0F0L, DATALONG[1]);

    setWords(DATALONG);
    sut.write(52L, 20, 0xABCDEL);
    assertWords(0xCDE000000000FF00L, 0x0000FF000000F0ABL);

    setWords(DATALONG);
    sut.write(52, 24, 0xABCDEF);
    assertWords(0xDEF000000000FF00L, 0x0000FF000000FABCL);
  }

  // helper methods
  /**
   * Assert that a word in the bit buffer.
   * @param expected expected words
   */
  private void assertWords(final long... expected) {
    assertArrayEquals(expected, getWords());
  }

  /**
   * Get the words array from a bit buffer instance.
   * @return the words array
   */
  private long[] getWords() {
    try {
      final Field wordsField = sut.getClass().getDeclaredField("words");
      wordsField.setAccessible(true);
      return (long[]) wordsField.get(sut);
    } catch(Exception e) {
      e.printStackTrace();
    }
    return new long[] {};
  }

  /**
   * Set the words array to the bit buffer instance.
   * @param words words array
   */
  private void setWords(final long[] words) {
    try {
      final Field wordsField = sut.getClass().getDeclaredField("words");
      wordsField.setAccessible(true);
      wordsField.set(sut, words.clone());
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
