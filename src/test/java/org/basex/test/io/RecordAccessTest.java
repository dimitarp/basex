package org.basex.test.io;

import static org.junit.Assert.*;
import static org.basex.util.Token.*;
import java.io.File;

import org.basex.io.random.RecordAccess;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RecordAccessTest {

  private File file;
  private RecordAccess sut;

  @Before
  public void setUp() throws Exception {
    file = File.createTempFile("ra-test", "basex");
    sut = new RecordAccess(file);
  }

  @Test
  public void test() throws Exception {
    final String test = "test";

    final long rid = sut.insert(token("test"));
    final long rid1 = sut.insert(token("test"));
    sut.close();

    sut = new RecordAccess(file);
    final String str = string(sut.select(rid));
    assertEquals("test", str);
  }
}
