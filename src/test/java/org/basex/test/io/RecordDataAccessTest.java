package org.basex.test.io;

import static org.basex.util.Token.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.basex.io.*;
import org.basex.io.random.*;
import org.basex.util.*;
import org.junit.*;

/**
 * Test class for record storage.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dimitar Popov
 */
public class RecordDataAccessTest {
  /** Test file. */
  private IOFile file;
  /** Object under test. */
  private RecordDataAccess sut;

  /**
   * Set up method.
   * @throws IOException I/O exception
   */
  @Before
  public void setUp() throws IOException {
    file = new IOFile(File.createTempFile("ra-test", ".basex"));
    file.file().deleteOnExit();
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
    assertTrue(lenAfterInsert > IO.BLOCKSIZE);


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
    Util.outln("Insert: " + Performance.getTime(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.nanoTime();
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
    Util.outln("Select: " + Performance.getTime(System.nanoTime() - sel, 1));
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
    Util.outln("Append: " + Performance.getTime(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.nanoTime();
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    }
    sut.close();
    Util.outln("Select: " + Performance.getTime(System.nanoTime() - sel, 1));
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
    Util.outln("Select: " + Performance.getTime(System.nanoTime() - ins, 1));

    // verify
    final long sel = System.nanoTime();
    da = new DataAccess(file);
    for(int i = 0; i < num; ++i) {
      assertEquals(prefix + i + suffix, string(da.readToken(rids[i])));
    }
    Util.outln("Select: " + Performance.getTime(System.nanoTime() - sel, 1));
  }

  /** Test using random operations with random strings. */
  @Test
  public void testRandom() {
    Random r = new Random(System.nanoTime());

    HashMap<Long, byte[]> existing = new HashMap<Long, byte[]>();
    HashSet<Long> deleted = new HashSet<Long>();

    for(int i = 0; i < 100000; ++i) {
      if(existing.isEmpty() || r.nextBoolean()) {
        final byte[] bytes = new byte[r.nextInt(15000)];
        r.nextBytes(bytes);

        final long rid = sut.insert(bytes);
        existing.put(rid, bytes);
        deleted.remove(rid);
      } else {
        long rid = 0L;
        int k = r.nextInt(existing.size());
        for(final Iterator<Long> s = existing.keySet().iterator(); k >= 0; --k) {
          rid = s.next();
        }
        sut.delete(rid);

        existing.remove(rid);
        deleted.add(rid);
      }
    }

    for(Long rid : existing.keySet()) {
      assertArrayEquals(existing.get(rid), sut.select(rid));
    }
  }
}
