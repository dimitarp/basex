package org.basex.test.io;

import static org.basex.util.BlockAccessUtil.*;
import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import org.basex.io.IO;
import org.basex.io.random.BlockManagedDataAccess;
import org.junit.Before;
import org.junit.Test;

/** Tests for class {@link BlockManagedDataAccess}. */
public final class BlockManagedDataAccessTest extends DataAccessTest {
  /** Instance of {@link BlockManagedDataAccess} under test. */
  private BlockManagedDataAccess bda;

  @Override
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("page", ".basex");
    final OutputStream o = new BufferedOutputStream(new FileOutputStream(file));
    try {
      o.write(1);
      for(int i = 1; i < IO.BLOCKSIZE; ++i) o.write(0);
      initialContent(o);
    } finally {
      o.close();
    }
    da = bda = new BlockManagedDataAccess(file);
  }



  /**
   * Test method {@link BlockManagedDataAccess#createBlock()}.
   * Create one data block.
   * @throws IOException I/O exception
   */
  @Test
  public void testCreateBlock1() throws IOException {
    assertEquals(1L, bda.createBlock());
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // the first byte should have only the first 2 bits set
      assertEquals(3, f.read());
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
    final long blocks = SEGMENTBLOCKS + n + 1;
    for(long b = 1L; b < blocks; ++b) assertEquals(b, bda.createBlock());
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      // the first header block should be full
      for(int i = 0; i < IO.BLOCKSIZE; ++i)
        assertEquals(BITMASK, f.read());

      // the second header block should not be full
      f.seek(position(headerBlock(1)));
      // the first byte should have only the first n bits set (+1 init block)
      assertEquals((1 << (n + 1)) - 1, f.read());
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
    for(int i = 0; i < initial; ++i) bda.createBlock();
    bda.deleteBlock(8L);
    bda.flush();

    final RandomAccessFile f = new RandomAccessFile(file, "r");
    try {
      assertEquals(BITMASK, f.read()); // 11 11 11 11
      assertEquals(6, f.read()); // 00 00 01 10
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
}
