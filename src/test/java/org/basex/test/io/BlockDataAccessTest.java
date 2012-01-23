package org.basex.test.io;

import static org.basex.util.BlockAccessUtil.*;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.basex.io.IO;
import org.basex.io.random.BlockDataAccess;
import org.junit.Before;
import org.junit.Test;

/** Tests for class {@link BlockDataAccess}. */
public final class BlockDataAccessTest extends DataAccessTest {
  /** Instance of {@link BlockDataAccess} under test. */
  private BlockDataAccess bda;
  /** Logical number of initially allocated blocks (i.e. data blocks). */
  private long initialBlocks;

  @Override
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("page", ".basex");
    final RandomAccessFile f = new RandomAccessFile(file, "rw");
    try {
      f.seek(IO.BLOCKSIZE);
      initialContent(f);

      initialBlocks = blocks(f.length()) - 1; // hacky: number of segment blocks
      // initialize the header block
      f.seek(0L);
      f.write(setFirst(initialBlocks));
      for(long i = initialBlocks; i < IO.BLOCKSIZE; ++i) f.write(0);
    } finally {
      f.close();
    }
    da = bda = new BlockDataAccess(file);
  }



  /**
   * Test method {@link BlockDataAccess#createBlock()}.
   * Create one data block.
   * @throws IOException I/O exception
   */
  @Test
  public void testCreateBlock1() throws IOException {
    assertEquals(initialBlocks, bda.createBlock());
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      assertEquals(setFirst(initialBlocks + 1), f.read());
    } finally {
      f.close();
    }
  }

  /**
   * Test method {@link BlockDataAccess#createBlock()}.
   * Create 32768+4 data blocks.
   * @throws IOException I/O exception
   */
  @Test
  public void testCreateBlock2() throws IOException {
    final int n = 4;
    final long blocks = initialBlocks + SEGMENTBLOCKS + n;
    for(long b = initialBlocks; b < blocks; ++b)
      assertEquals(b, bda.createBlock());
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // the first header block should be full
      for(int i = 0; i < IO.BLOCKSIZE; ++i)
        assertEquals(BITMASK, f.read());

      // the second header block should not be full
      f.seek(position(headerBlock(1L)));
      // the first byte should have only the first n bits set (+ init blocks)
      assertEquals((1 << (n + initialBlocks)) - 1, f.read());
    } finally {
      f.close();
    }
  }

   /**
    * Test method {@link BlockDataAccess#deleteBlock(long)}.
    * Create 10 blocks and delete block with number 8.
    * @throws IOException I/O exception
    */
  @Test
  public void testDeleteBlock1() throws IOException {
    final int n = 10;
    for(int i = 0; i < n; ++i) bda.createBlock();
    bda.deleteBlock(8L);
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // hacky!
      assertEquals(BITMASK, f.read()); // 11 11 11 11
      assertEquals(14, f.read()); // 00 00 11 10
    } finally {
      f.close();
    }
  }

  /**
   * Check that the test file {@link #file} has the specified unsigned bytes at
   * the specified position.
   * @param pos file position
   * @param bytes expected unsigned bytes
   * @throws IOException I/O exception
   */
  @Override
  protected void assertContent(final long pos, final int[] bytes)
      throws IOException {
    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      f.seek(physicalPosition(pos));
      for(int i = 0; i < bytes.length; ++i) assertEquals(bytes[i], f.read());
    } finally {
      f.close();
    }
  }

  /**
   * Create a bit mask with the first n bits set.
   * @param n number of bits to set
   * @return bit mask
   */
  private static int setFirst(final long n) {
    return BITMASK >>> (Byte.SIZE - n);
  }
}
