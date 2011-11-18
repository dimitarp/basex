package org.basex.test.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.basex.io.random.DataAccess;
import org.basex.util.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for class {@link DataAccess}. */
public class DataAccessTest {
  private static final String STR = "string with characters: öäü10";
  private static final byte BYTE = Byte.MIN_VALUE;
  private static final long LONG = 1 << 5 * Byte.SIZE;
  private static final int INT = Integer.MAX_VALUE;
  private static final int CINT4 = 0x3FFFFFFF + 1;
  private static final int CINT3 = 0x3FFF + 1;
  private static final int CINT2 = 0x3F + 1;
  private static final int CINT1 = 0x3F;


  private static final byte[] STR_BIN = Token.token(STR);
  private static final byte[] BYTE_BIN = new byte[] { BYTE };
  private static final byte[] LONG_BIN = new byte[] {
    (byte) (LONG >>> 32),
    (byte) (LONG >>> 24),
    (byte) (LONG >>> 16),
    (byte) (LONG >>> 8),
    (byte) LONG
  };
  private static final byte[] INT_BIN = new byte[] {
    (byte) (INT >>> 24),
    (byte) (INT >>> 16),
    (byte) (INT >>> 8),
    (byte) INT
  };
  private static final byte[] CINT4_BIN = new byte[] {
    (byte) 0xC0,
    (byte) (CINT4 >>> 24),
    (byte) (CINT4 >>> 16),
    (byte) (CINT4 >>> 8),
    (byte) CINT4
  };
  private static final byte[] CINT3_BIN = new byte[] {
    (byte) (CINT3 >>> 24 | 0x80),
    (byte) (CINT3 >>> 16),
    (byte) (CINT3 >>> 8),
    (byte) CINT3
  };
  private static final byte[] CINT2_BIN = new byte[] {
    (byte) (CINT2 >>> 8 | 0x40),
    (byte) CINT2
  };
  private static final byte[] CINT1_BIN = new byte[] {
    (byte) CINT1
  };

  private byte[] numToByteArray(final int v) {
    if(v < 0 || v > 0x3FFFFFFF) {
      return new byte[] {
          (byte) 0xC0,
          (byte) (v >>> 24),
          (byte) (v >>> 16),
          (byte) (v >>> 8),
          (byte) v
      };
    } else if(v > 0x3FFF) {
      return new byte[] {
          (byte) (v >>> 24 | 0x80),
          (byte) (v >>> 16),
          (byte) (v >>> 8),
          (byte) v
      };
    } else if(v > 0x3F) {
      return new byte[] {
          (byte) (v >>> 8 | 0x40),
          (byte) v
      };
    } else {
      return new byte [] { (byte) v };
    }
  }

  /** Temporary file. */
  private File file;
  /** Instance under test. */
  private DataAccess da;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("page", ".basex");
    FileOutputStream out = new FileOutputStream(file);
    try {
      out.write(STR_BIN);
      out.write(BYTE_BIN);
    } finally {
      out.close();
    }

    da = new DataAccess(file);
  }

  /**
   * Tear down method.
   * @throws IOException I/O exception
   */
  @After
  public void tearDown() throws IOException {
    da.close();
    file.delete();
  }

  /**
   * Test method for {@link DataAccess#cursor()}.
   */
  @Test
  public final void testCursor() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#length()}.
   */
  @Test
  public final void testLength() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#more()}.
   */
  @Test
  public final void testMore() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read1(long)}.
   */
  @Test
  public final void testRead1Long() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read1()}.
   */
  @Test
  public final void testRead1() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read4(long)}.
   */
  @Test
  public final void testRead4Long() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read4()}.
   */
  @Test
  public final void testRead4() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read5(long)}.
   */
  @Test
  public final void testRead5Long() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#read5()}.
   */
  @Test
  public final void testRead5() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readNum(long)}.
   */
  @Test
  public final void testReadNumLong() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readToken(long)}.
   */
  @Test
  public final void testReadTokenLong() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readToken()}.
   */
  @Test
  public final void testReadToken() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readBytes(long, int)}.
   */
  @Test
  public final void testReadBytesLongInt() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readBytes(int)}.
   */
  @Test
  public final void testReadBytesInt() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#cursor(long)}.
   */
  @Test
  public final void testCursorLong() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#readNum()}.
   */
  @Test
  public final void testReadNum() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#write1(int)}.
   */
  @Test
  public final void testWrite1() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#write4(long, int)}.
   */
  @Test
  public final void testWrite4LongInt() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#write4(int)}.
   */
  @Test
  public final void testWrite4Int() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#writeToken(long, byte[])}.
   */
  @Test
  public final void testWriteToken() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#writeNum(int)}.
   */
  @Test
  public final void testWriteNum() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link DataAccess#free(long, int)}.
   */
  @Test
  public final void testFree() {
    fail("Not yet implemented"); // TODO
  }
}
