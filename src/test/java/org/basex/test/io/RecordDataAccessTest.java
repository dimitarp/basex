package org.basex.test.io;

import static org.basex.util.Token.*;
import static org.junit.Assert.*;

import java.io.File;

import org.basex.io.random.DataAccess;
import org.basex.io.random.RecordDataAccessOld;
import org.basex.io.random.RecordDataAccess;
import org.basex.util.Performance;
import org.junit.Before;
import org.junit.Test;

public class RecordDataAccessTest {

  private File file;
  private RecordDataAccess sut;

  @Before
  public void setUp() throws Exception {
    file = File.createTempFile("ra-test", "basex");
    file.deleteOnExit();
    sut = new RecordDataAccess(file);
  }

  @Test
  public void test() throws Exception {
    final String test = "test";

    final long rid = sut.insert(token("test"));
    final long rid1 = sut.insert(token("test23423"));
    sut.close();

    sut = new RecordDataAccess(file);
    assertEquals("test", string(sut.select(rid)));
    assertEquals("test23423", string(sut.select(rid1)));
  }

  @Test
  public void test2() throws Exception {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";

    final int num = 1000000;
    final long[] rids = new long[num];


    final long insertStart = System.currentTimeMillis();
    for(int i = 0; i < num; ++i)
      rids[i] = sut.insert(token(prefix + i + suffix));
    sut.close();
    final long insertTime = System.currentTimeMillis() - insertStart;
    System.out.println("Insert: " + insertTime +  " ms");


    final long selectStart = System.currentTimeMillis();
    sut = new RecordDataAccess(file);
    for(int i = 0; i < num; ++i)
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));
    sut.close();
    final long selectTime = System.currentTimeMillis() - selectStart;
    System.out.println("Select: " + selectTime +  " ms");
  }

  @Test
  public void testOld() throws Exception {
    sut.close();
    DataAccess da = new DataAccess(file);

    final String prefix = "testPrefix";
    final String suffix = "testSuffix";

    final int num = 1000000;
    final long[] rids = new long[num];

    for(int i = 0; i < num; ++i) {
      rids[i] = da.cursor();
      da.writeToken(token(prefix + i + suffix));
    }
    da.close();

    da = new DataAccess(file);
    for(int i = 0; i < num; ++i)
      assertEquals(prefix + i + suffix, string(da.readToken(rids[i])));
  }
}
