package org.basex.test.io;

import static org.basex.io.random.BlockManagedDataAccess.*;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.basex.io.IO;
import org.basex.io.random.BlockManagedDataAccess;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for class {@link BlockManagedDataAccess}. */
public final class BlockManagedDataAccessTest {
  /** Number of blocks, managed by one header. */
  private static final long BLOCKS = 1 << BLOCKSPERHEADERPOWER;
  /** Temporary file. */
  private File file;
  /** Instance under test. */
  private BlockManagedDataAccess da;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("page", ".basex");
    da = new BlockManagedDataAccess(file);
  }

  /**
   * Clean up method.
   * @throws IOException I/O exception
   */
  @After
  public void cleanUp() throws IOException {
    da.close();
    file.delete();
  }



  /**
   * Test method {@link BlockManagedDataAccess#createBlock()}.
   * Create one data block.
   * @throws IOException I/O exception
   */
  @Test
  public void testCreateBlock1() throws IOException {
    assertEquals(0L, da.createBlock());
    da.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // the first byte should have only the first bit set
      assertEquals(1, f.read());
    } finally {
      f.close();
    }
  }

  /**
   * Test method {@link BlockManagedDataAccess#createBlock()}.
   * Create 32768+4 data blocks.
   * @throws IOException I/O exception
   */
  @Test
  public void testCreateBlock2() throws IOException {
    final int n = 4;
    final long blocks = BLOCKSPERHEADER + n;
    for(long b = 0L; b < blocks; ++b) assertEquals(b, da.createBlock());
    da.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // the first header block should be full
      for(int i = 0; i < IO.BLOCKSIZE; ++i)
        assertEquals(BITMASK, f.read());

      // the second header block should not be full
      f.seek(position(headerBlock(1)));
      // the first byte should have only the first n bits set
      assertEquals((1 << n) - 1, f.read());
    } finally {
      f.close();
    }
  }

   /**
    * Test method {@link BlockManagedDataAccess#deleteBlock(long)}.
    * Create 10 blocks and delete block with number 8.
    * @throws IOException I/O exception
    */
  @Test
  public void testDeleteBlock1() throws IOException {
    final int initial = 10;
    for(int i = 0; i < initial; ++i) da.createBlock();
    da.deleteBlock(8L);
    da.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      assertEquals(BITMASK, f.read());
      assertEquals(2, f.read());
    } finally {
      f.close();
    }
  }



  // static methods tests
  /**
   * Test method {@link BlockManagedDataAccess#dataBlock(long)}.
   * Get physical block number of block with logical number 0.
   */
  @Test
  public void testDataBlock1() {
    assertEquals(1L, dataBlock(0L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlock(long)}.
   * Get physical block number of block with logical number 32767.
   */
  @Test
  public void testDataBlock2() {
    assertEquals(BLOCKS, dataBlock(BLOCKS - 1));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlock(long)}.
   * Get physical block number of block with logical number 32768.
   */
  @Test
  public void testDataBlock3() {
    assertEquals(BLOCKS + 2L, dataBlock(BLOCKS));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlock(long)}.
   * Get physical block number of block with logical number 32768+10.
   */
  @Test
  public void testDataBlock4() {
    assertEquals(BLOCKS + 10L + 2L, dataBlock(BLOCKS + 10L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlock(long)}.
   * Get physical block number of block with logical number 65536+10.
   */
  @Test
  public void testDataBlock5() {
    assertEquals(2 * BLOCKS + 10L + 3L, dataBlock(2 * BLOCKS + 10L));
  }



  // Tests for method {@link BlockManagedDataAccess#headerBlockFor(long)}. */
  /**
   * Test method {@link BlockManagedDataAccess#headerBlockFor(long)}.
   * Get physical header block number of block with logical number 0.
   */
  @Test
  public void testHeaderBlockFor1() {
    assertEquals(0L, headerBlockFor(0L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlockFor(long)}.
   * Get physical header block number of block with logical number 32767.
   */
  @Test
  public void testHeaderBlockFor2() {
    assertEquals(0L, headerBlockFor(BLOCKS - 1));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlockFor(long)}.
   * Get physical header block number of block with logical number 32768.
   */
  @Test
  public void testHeaderBlockFor3() {
    assertEquals(BLOCKS + 1L, headerBlockFor(BLOCKS));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlockFor(long)}.
   * Get physical header block number of block with logical number 32768+10.
   */
  @Test
  public void testHeaderBlockFor4() {
    assertEquals(BLOCKS + 1L, headerBlockFor(BLOCKS + 10L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlockFor(long)}.
   * Get physical header block number of block with logical number 65536+10.
   */
  @Test
  public void testHeaderBlockFor5() {
    assertEquals(2 * BLOCKS + 2L, headerBlockFor(2 * BLOCKS + 10L));
  }



  /**
   * Test method {@link BlockManagedDataAccess#dataBlockOffset(long)}.
   * Get offset from header of block with logical number 0.
   */
  @Test
  public void testDataBlockOffset1() {
    assertEquals(0L, dataBlockOffset(0L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlockOffset(long)}.
   * Get offset from header of block with logical number 32767.
   */
  @Test
  public void testDataBlockOffset2() {
    assertEquals(BLOCKS - 1L, dataBlockOffset(BLOCKS - 1L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlockOffset(long)}.
   * Get offset from header of block with logical number 32768.
   */
  @Test
  public void testDataBlockOffset3() {
    assertEquals(0L, dataBlockOffset(BLOCKS));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlockOffset(long)}.
   * Get offset from header of block with logical number 32768+10.
   */
  @Test
  public void testDataBlockOffset4() {
    assertEquals(10L, dataBlockOffset(BLOCKS + 10L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#dataBlockOffset(long)}.
   * Get offset from header of block with logical number 32768+10.
   */
  @Test
  public void testDataBlockOffset5() {
    assertEquals(10L, dataBlockOffset(2 * BLOCKS + 10L));
  }



  /**
   * Test method {@link BlockManagedDataAccess#headerBlock(long)}.
   * Get physical block number for header block 0.
   */
  @Test
  public void testHeaderBlock1() {
    assertEquals(0L, headerBlock(0L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlock(long)}.
   * Get physical block number for header block 1.
   */
  @Test
  public void testHeaderBlock2() {
    assertEquals(32769L, headerBlock(1L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#headerBlock(long)}.
   * Get physical block number for header block 2.
   */
  @Test
  public void testHeaderBlock3() {
    assertEquals(65538L, headerBlock(2L));
  }




  /**
   * Test method {@link BlockManagedDataAccess#divRoundUp(long, long)}.
   * Test division without remainder.
   */
  @Test
  public void testDivRoundUp1() {
    final long factor = 2L;
    assertEquals(factor, divRoundUp(factor * 10L, 10L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#divRoundUp(long, long)}.
   * Test division with remainder.
   */
  @Test
  public void testDivRoundUp2() {
    final long factor = 2L;
    assertEquals(factor + 1L, divRoundUp(factor * 10L, 10L - 1L));
  }

  /**
   * Test method {@link BlockManagedDataAccess#divRoundUp(long, long)}.
   * Test division with big numbers.
   */
  @Test
  public void testDivRoundUp3() {
    assertEquals(2L, divRoundUp(Long.MAX_VALUE, Long.MAX_VALUE - 1));
  }



  /**
   * Test method {@link BlockManagedDataAccess#position(long)}.
   * Test calculation of a block position.
   */
  @Test
  public void testPosition() {
    final long block = 10L;
    assertEquals(block * IO.BLOCKSIZE, position(block));
  }



  /**
   * Test method {@link BlockManagedDataAccess#blocks(long)}.
   * Get number of blocks for a file with length 4095.
   */
  @Test
  public void testBlocks1() {
    assertEquals(1L, blocks(IO.BLOCKSIZE - 1));
  }

  /**
   * Test method {@link BlockManagedDataAccess#blocks(long)}.
   * Get number of blocks for a file with length 4096.
   */
  @Test
  public void testBlocks2() {
    assertEquals(1L, blocks(IO.BLOCKSIZE));
  }

  /**
   * Test method {@link BlockManagedDataAccess#blocks(long)}.
   * Get number of blocks for a file with length 4096.
   */
  @Test
  public void testBlocks3() {
    assertEquals(2L, blocks(IO.BLOCKSIZE + 1));
  }

  /**
   * Test method {@link BlockManagedDataAccess#blocks(long)}.
   * Get number of blocks for a file with length 2<sup>63</sup> - 1.
   */
  @Test
  public void testBlocks4() {
    assertEquals(1L << (63 - IO.BLOCKPOWER), blocks(Long.MAX_VALUE));
  }



  /**
   * Test method {@link BlockManagedDataAccess#modulo2(long, long)}.
   * Calculate 10 % 8.
   */
  @Test
  public void testModulo2() {
    assertEquals(2L, modulo2(10L, 3));
  }



  /**
   * Test method {@link BlockManagedDataAccess#set(int, long)}.
   * Set bit 8+8+3 of word 10100010 (=162)
   */
  @Test
  public void testSet1() {
    assertEquals(170, set(162, 2 * Byte.SIZE + 3));
  }

  /**
   * Test method {@link BlockManagedDataAccess#set(int, long)}.
   * Clear bit 8+8+3 of word 10101010 (=170)
   */
  @Test
  public void testClear1() {
    assertEquals(162, clear(170, 2 * Byte.SIZE + 3));
  }
}
