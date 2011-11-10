package org.basex.test.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.io.random.BlockDataAccess;
import org.basex.util.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test page data access.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class PagedDataAccessTest {
  /** Test string. */
  private static final String STR = "test";
  /** Test token. */
  private static final byte[] TOKEN = Token.token(STR);
  /** Test integer. */
  private static final int INT = Integer.MIN_VALUE;
  /** Temporary file. */
  private File file;
  /** Instance under test. */
  private BlockDataAccess da;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("page", ".basex");
    da = new BlockDataAccess(file);
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
   * Test empty file initialization.
   * @throws IOException I/O exception
   */
  @Test
  public void testInit() throws IOException {
    da.flush();

    // asserts
    assertTrue(file.exists());
    // two longs should have been written
    assertEquals(IO.BLOCKSIZE, file.length());
  }

  /**
   * Test writing to the first page of the file.
   * @throws IOException I/O exception
   */
  @Test
  public void testWriteSinglePage() throws IOException {
    long pos = 0L;

    da.writeToken(pos, TOKEN);
    pos += TOKEN.length;

    da.write4(pos, INT);
    pos += Integer.SIZE;

    da.writeToken(pos, TOKEN);
    pos += TOKEN.length;

    da.flush();

    // asserts
    assertTrue(file.exists());
    // two longs should have been written
    assertEquals(2 * IO.BLOCKSIZE, file.length());
  }
}
