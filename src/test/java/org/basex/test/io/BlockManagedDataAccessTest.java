package org.basex.test.io;

import static org.basex.util.BlockAccessUtil.*;
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
    final long blocks = SEGMENTBLOCKS + n;
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
}
