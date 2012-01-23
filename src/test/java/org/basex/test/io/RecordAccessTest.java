package org.basex.test.io;

import static org.basex.util.Token.*;
import static org.junit.Assert.*;

import java.io.File;

import org.basex.io.random.RecordAccess;
import org.junit.Before;
import org.junit.Test;

public class RecordAccessTest {

  private File file;
  private RecordAccess sut;

  @Before
  public void setUp() throws Exception {
    file = File.createTempFile("ra-test", "basex");
    file.deleteOnExit();
    sut = new RecordAccess(file);
  }

  @Test
  public void test() throws Exception {
    final String test = "test";

    final long rid = sut.insert(token("test"));
    final long rid1 = sut.insert(token("test23423"));
    sut.close();

    sut = new RecordAccess(file);
    assertEquals("test", string(sut.select(rid)));
    assertEquals("test", string(sut.select(rid1)));
  }

  @Test
  public void test2() throws Exception {
    final String prefix = "testPrefix";
    final String suffix = "testSuffix";

    final int num = 100000;
    final long[] rids = new long[num];

    for(int i = 0; i < num; ++i)
      rids[i] = sut.insert(token(prefix + i + suffix));
    sut.close();

    sut = new RecordAccess(file);
    for(int i = 0; i < 93522; ++i)
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));

    for(int i = 93522; i < num; ++i)
      assertEquals(prefix + i + suffix, string(sut.select(rids[i])));

  }
}
