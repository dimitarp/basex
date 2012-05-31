package org.basex.test.io;

import static org.basex.util.Token.*;

import java.io.*;
import java.util.*;

import org.basex.io.*;
import org.basex.io.random.*;
import org.junit.*;

public class RecordDataAccessPerfTest {
  /** Test file. */
  private IOFile fileNew;
  /** Object under test. */
  private RecordDataAccess daNew;
  /** Test file. */
  private IOFile fileOld;
  /** Object under test. */
  private DataAccess daOld;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    fileNew = new IOFile(File.createTempFile("ra-test1", ".basex"));
    //fileNew = new IOFile(new File("ra-test1.basex"));
    fileNew.file().deleteOnExit();
    daNew = new RecordDataAccess(fileNew);

    fileOld = new IOFile(File.createTempFile("ra-test2", ".basex"));
    //fileOld = new IOFile(new File("ra-test2.basex"));
    fileOld.file().deleteOnExit();
    daOld = new DataAccess(fileOld);
  }

  private long appendNew(final byte[][] tokens, final long[] rids) {
    final long start = System.nanoTime();
    for(int i = 0; i < tokens.length; ++i) rids[i] = daNew.append(tokens[i]);
    return System.nanoTime() - start;
  }

  private long appendOld(final byte[][] tokens, final long[] rids) {
    final long start = System.nanoTime();
    for(int i = 0; i < tokens.length; ++i) {
      rids[i] = daOld.cursor();
      daOld.writeToken(tokens[i]);
    }
    return System.nanoTime() - start;
  }

  private long selectNew(final long[] rids) {
    final long start = System.nanoTime();
    for(int i = 0; i < rids.length; ++i) daNew.select(rids[i]);
    return System.nanoTime() - start;
  }

  private long selectOld(final long[] rids) {
    final long start = System.nanoTime();
    for(int i = 0; i < rids.length; ++i) daOld.readToken(rids[i]);
    return System.nanoTime() - start;
  }

  private static void generate(final byte[][] tokens) {
    final Random r = new Random(System.nanoTime());
    for(int i = 0; i < tokens.length; ++i)
      tokens[i] = token("testString" + r.nextLong() + "test");
  }

  private static long[] shuffle(final long[] array, final int n) {
    final long[] newarray = new long[n];
    final Random random = new Random(System.nanoTime());
    for(int i = 0; i < newarray.length; ++i) {
      newarray[i] = array[random.nextInt(array.length)];
    }
    return newarray;
  }

  @Test
  public void testAppendPerf() {
    final int steps = 20;
    final int stepSize = 1000000;

    final byte[][] tokens = new byte[stepSize][];
    final long[] ridsNew = new long[stepSize];
    final long[] ridsOld = new long[stepSize];
    generate(tokens);
    for(int i = 0; i <= steps; ++i) {
      final long timeNew = appendNew(tokens, ridsNew);
      final long timeOld = appendOld(tokens, ridsOld);
      System.out.println(i * stepSize + "\t" + timeNew / 1000000.0 + "\t" + timeOld / 1000000.0);
    }
    return;
  }

  @Test
  public void testSelectPerf() {
    final int steps = 20;
    final int stepSize = 1000000;
    final int size = 10000;

    final byte[][] tokens = new byte[stepSize][];
    final long[] ridsNew = new long[stepSize];
    final long[] ridsOld = new long[stepSize];
    generate(tokens);
    for(int i = 0; i <= steps; ++i) {
      appendNew(tokens, ridsNew);
      appendOld(tokens, ridsOld);

      final long timeNew = selectNew(shuffle(ridsNew, size));
      final long timeOld = selectOld(shuffle(ridsOld, size));

      System.out.println(i * stepSize + "\t" + timeNew / 1000000.0 + "\t" + timeOld / 1000000.0);
    }
  }
}
