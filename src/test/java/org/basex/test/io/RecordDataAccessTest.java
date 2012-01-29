package org.basex.test.io;

import static org.basex.util.Token.*;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import org.basex.io.random.DataAccess;
import org.basex.io.random.RecordDataAccess;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for record storage.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dimitar Popov
 */
public class RecordDataAccessTest {
  /** Test file. */
  private File file;
  /** Object under test. */
  private RecordDataAccess sut;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    file = File.createTempFile("ra-test", ".basex");
    file.deleteOnExit();
    sut = new RecordDataAccess(file);
  }

  /**
   * Test inserting two records and the reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testInsertSelectSmall() throws IOException {
    final String v1 = "testjkfdf";
    final String v2 = "test3453";

    // insert
    final long rid1 = sut.insert(token(v1));
    final long rid2 = sut.insert(token(v2));
    sut.close();

    // verify
    sut = new RecordDataAccess(file);
    assertEquals(v1, string(sut.select(rid1)));
    assertEquals(v2, string(sut.select(rid2)));
  }

  /**
   * Test inserting records and the reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testInsertSelectBig() throws IOException {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 100000;
    final long[] rids = new long[num];

    // insert
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.insert(token(prefix + i + suffix));
    }
    sut.close();

    // verify
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
  }

  /**
   * Test appending records and the reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testAppendSelectBig() throws IOException {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 100000;
    final long[] rids = new long[num];

    // append
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.append(token(prefix + i + suffix));
    }
    sut.close();

    // verify
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
  }

  /**
   * Test inserting, deleting, and re-inserting records and then reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testInsertDeleteInsert() throws IOException {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 100000;
    final long[] rids = new long[num];

    // insert
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.insert(token(prefix + i + suffix));
    }
    sut.close();

    // verify file length
    final long lenAfterInsert = file.length();
    assertTrue(lenAfterInsert > 4096);


    // delete
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      sut.delete(rids[i]);
    }
    sut.close();

    // verify file length
    final long lenAfterDelete = file.length();
    assertEquals(lenAfterInsert, lenAfterDelete);


    // re-insert
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.insert(token(prefix + i + suffix));
    }
    sut.close();

    // verify file length
    final long lenAfterReInsert = file.length();
    assertEquals(lenAfterInsert, lenAfterReInsert);


    // select
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
  }

  /**
   * Performance test inserting records and reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testPerfInsertSelect() throws IOException {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 10000;
    final long[] rids = new long[num];

    // insert
    final long ins = System.nanoTime();
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.insert(token(prefix + i + suffix));
    }
    sut.close();
    Util.outln("Insert: " + Performance.getTimer(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.currentTimeMillis();
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
    Util.outln("Select: " + Performance.getTimer(System.nanoTime() - sel, 1));
  }

  /**
   * Performance test appending records and reading them.
   * @throws IOException I/O exception
   */
  @Test
  public void testPerfAppendSelect() throws IOException {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 1000000;
    final long[] rids = new long[num];

    // append
    final long ins = System.nanoTime();
    for(int i = 0; i < num; ++i) {
      rids[i] = sut.append(token(prefix + i + suffix));
    }
    sut.close();
    Util.outln("Append: " + Performance.getTimer(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.currentTimeMillis();
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
    Util.outln("Select: " + Performance.getTimer(System.nanoTime() - sel, 1));
  }

  /**
   * Performance test appending records and reading them using the original
   * {@link DataAccess} class.
   * @throws IOException I/O exception
   */
  @Test
  public void testPerfAppendSelectOld() throws IOException {
    sut.close();
    DataAccess da = new DataAccess(file);
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";
    final int num = 1000000;
    final long[] rids = new long[num];

    // append
    final long ins = System.nanoTime();
    for(int i = 0; i < num; ++i) {
      rids[i] = da.cursor();
      da.writeToken(token(prefix + i + suffix));
    }
    da.close();
    Util.outln("Select: " + Performance.getTimer(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.currentTimeMillis();
    da = new DataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(da.readToken(rids[i])));
    }
    Util.outln("Select: " + Performance.getTimer(System.nanoTime() - sel, 1));
  }
}
