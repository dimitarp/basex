package org.basex.test.util;

import static org.basex.util.BlockAccessUtil.*;
import static org.junit.Assert.*;
import org.basex.io.IO;
import org.basex.util.BlockAccessUtil;
import org.junit.Test;

/**
 * Tests for class {@link BlockAccessUtil}.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockAccessUtilTest {
  /** Test method {@link BlockAccessUtil#dataBlock(long)}. */
  @Test
  public void testDataBlock() {
    // Get physical block number of block with logical number 0.
    assertEquals(1L, dataBlock(0L));
    // Get physical block number of block with logical number 32767.
    assertEquals(SEGMENTBLOCKS, dataBlock(SEGMENTBLOCKS - 1));
    // Get physical block number of block with logical number 32768.
    assertEquals(SEGMENTBLOCKS + 2L, dataBlock(SEGMENTBLOCKS));
    // Get physical block number of block with logical number 32768+10.
    assertEquals(SEGMENTBLOCKS + 12L, dataBlock(SEGMENTBLOCKS + 10L));
    // Get physical block number of block with logical number 65536+10.
    assertEquals(2 * SEGMENTBLOCKS + 13L, dataBlock(2 * SEGMENTBLOCKS + 10L));
  }

  /** Test method {@link BlockAccessUtil#headerBlockFor(long)}. */
  @Test
  public void testHeaderBlockFor() {
    // Get physical header block number of block with logical number 0.
    assertEquals(0L, headerBlockFor(0L));
    // Get physical header block number of block with logical number 32767.
    assertEquals(0L, headerBlockFor(SEGMENTBLOCKS - 1));
    // Get physical header block number of block with logical number 32768.
    assertEquals(SEGMENTBLOCKS + 1L, headerBlockFor(SEGMENTBLOCKS));
    // Get physical header block number of block with logical number 32768+10.
    assertEquals(SEGMENTBLOCKS + 1L, headerBlockFor(SEGMENTBLOCKS + 10L));
    // Get physical header block number of block with logical number 65536+10.
    assertEquals(2 * SEGMENTBLOCKS + 2, headerBlockFor(2 * SEGMENTBLOCKS + 10));
  }

  /** Test method {@link BlockAccessUtil#dataBlockOffset(long)}. */
  @Test
  public void testDataBlockOffset() {
    // Get offset from header of block with logical number 0.
    assertEquals(0L, dataBlockOffset(0L));
    // Get offset from header of block with logical number 32767.
    assertEquals(SEGMENTBLOCKS - 1L, dataBlockOffset(SEGMENTBLOCKS - 1L));
    // Get offset from header of block with logical number 32768.
    assertEquals(0L, dataBlockOffset(SEGMENTBLOCKS));
    // Get offset from header of block with logical number 32768+10.
    assertEquals(10L, dataBlockOffset(SEGMENTBLOCKS + 10L));
    // Get offset from header of block with logical number 32768+10.
    assertEquals(10L, dataBlockOffset(2 * SEGMENTBLOCKS + 10L));
  }

  /** Test method {@link BlockAccessUtil#headerBlock(long)}. */
  @Test
  public void testHeaderBlock() {
    // Get physical block number for header block 0.
    assertEquals(0L, headerBlock(0L));
    // Get physical block number for header block 1.
    assertEquals(32769L, headerBlock(1L));
    // Get physical block number for header block 2.
    assertEquals(65538L, headerBlock(2L));
  }

  /** Test method {@link BlockAccessUtil#divRoundUp(long, long)}. */
  @Test
  public void testDivRoundUp() {
    // Test division without remainder.
    long factor = 2L;
    assertEquals(factor, divRoundUp(factor * 10L, 10L));
    // Test division with remainder.
    factor = 2L;
    assertEquals(factor + 1L, divRoundUp(factor * 10L, 10L - 1L));
    // Test division with big numbers.
    assertEquals(2L, divRoundUp(Long.MAX_VALUE, Long.MAX_VALUE - 1));
  }

  /** Test method {@link BlockAccessUtil#position(long)}. */
  @Test
  public void testPosition() {
    final long block = 10L;
    assertEquals(block * IO.BLOCKSIZE, position(block));
  }

  /** Test method {@link BlockAccessUtil#blocks(long)}. */
  @Test
  public void testBlocks() {
    // Get number of blocks for a file with length 4095.
    assertEquals(1L, blocks(IO.BLOCKSIZE - 1));
    // Get number of blocks for a file with length 4096.
    assertEquals(1L, blocks(IO.BLOCKSIZE));
    // Get number of blocks for a file with length 4096.
    assertEquals(2L, blocks(IO.BLOCKSIZE + 1));
    // Get number of blocks for a file with length 2<sup>63</sup> - 1.
    assertEquals(1L << (63 - IO.BLOCKPOWER), blocks(Long.MAX_VALUE));
  }

  /** Test method {@link BlockAccessUtil#modulo2(long, long)}. */
  @Test
  public void testModulo2() {
    // Calculate 10 % 8.
    assertEquals(10L % 8, modulo2(10L, 8));
  }

  /** Test method {@link BlockAccessUtil#set(int, long)}. */
  @Test
  public void testSet() {
    // Set bit 8+8+3 of word 10100010 (=162)
    assertEquals(170, set(162, 2 * Byte.SIZE + 3));
    // Clear bit 8+8+3 of word 10101010 (=170)
    assertEquals(162, clear(170, 2 * Byte.SIZE + 3));
  }

  /** Test method {@link BlockAccessUtil#logicalPosition(long)}. */
  @Test
  public void testLogicalPosition() {
    long pos = 0L;
    assertEquals(pos, logicalPosition(IO.BLOCKSIZE + pos));

    pos = 10L;
    assertEquals(pos, logicalPosition(IO.BLOCKSIZE + pos));

    pos = SEGMENTSIZE + 10L;
    assertEquals(pos, logicalPosition(2 * IO.BLOCKSIZE + pos));
  }

  /** Test method {@link BlockAccessUtil#physicalPosition(long)}. */
  @Test
  public void testPhysicalPosition() {
    long pos = 0L;
    assertEquals(IO.BLOCKSIZE + pos, physicalPosition(pos));

    pos = 10L;
    assertEquals(IO.BLOCKSIZE + pos, physicalPosition(pos));

    pos = SEGMENTSIZE + 10L;
    assertEquals(2 * IO.BLOCKSIZE + pos, physicalPosition(pos));
  }
}
