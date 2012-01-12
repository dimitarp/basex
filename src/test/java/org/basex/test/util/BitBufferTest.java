package org.basex.test.util;

import static org.junit.Assert.*;
import static org.basex.util.BitBuffer.*;

import org.basex.util.BitBuffer;
import org.junit.Test;

/**
 * Tests for methods of class {@link BitBuffer}.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public final class BitBufferTest {
  private static final byte[] DATA = new byte[] {
    (byte) 0x00, (byte) 0xFF,
    (byte) 0x00, (byte) 0x00,
    (byte) 0x00, (byte) 0x00,
    (byte) 0x00, (byte) 0xFF,

    (byte) 0xFF, (byte) 0xF0,
    (byte) 0x00, (byte) 0x00,
    (byte) 0x00, (byte) 0xFF,
    (byte) 0x00, (byte) 0x00
  };

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

  /** Test for {@link BitBuffer#read(long, int)}. */
  @Test
  public void testRead() {
    final BitBuffer sut = new BitBuffer(DATA.length * 8);
    sut.init(DATA);

    assertEquals(0xFFL, sut.read(8, 8));
    assertEquals(0xFFFFL, sut.read(56, 16));
    assertEquals(0x00FFFFL, sut.read(56, 20));
    assertEquals(0xF0FFFFL, sut.read(56, 24));
    assertEquals(0xFF000000F0FFFFL, sut.read(56, 56));
  }
}
