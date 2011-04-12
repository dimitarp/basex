package org.basex.test.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.XQuery;
import org.basex.data.Data;
import org.basex.data.MemData;
import org.basex.io.IOContent;

/** Test index updates when using memory storage ({@link MemData}). */
public class MemDataTest {
  /** XML document. */
  private static final byte[] XML = "<a><b>test</b><c/></a>".getBytes();
  /** Database context. */
  private static final Context CTX = new Context();
  /** Tested {@link MemData} instance. */
  private Data data;

  /**
   * Set up method; executed before each test.
   * @throws IOException should never be thrown
   */
  @Before
  public void setUp() throws IOException {
    data = CreateDB.xml(new IOContent(XML), CTX);
    CTX.openDB(data);
  }

  /** Clean up method; executed after each test. */
  @After
  public void end() {
    CTX.closeDB();
    data = null;
  }

  /**
   * Replace value update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testReplaceValue() throws BaseXException {
    new XQuery("replace value of node /a/b with 'test2'").execute(CTX);
    System.out.println(new XQuery("//*[text() = 'test2']").execute(CTX));
  }

  /**
   * Replace node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testReplaceNode() throws BaseXException {
    new XQuery("replace node /a/b with <d f='test4'/>").execute(CTX);
    System.out.println(new XQuery("//*[@f = 'test4']").execute(CTX));
  }

  /**
   * Insert node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testInsertNode() throws BaseXException {
    new XQuery("insert node <d>test3</d> as first into /a").execute(CTX);
    System.out.println(new XQuery("//*[text() = 'test3']").execute(CTX));
  }

  /**
   * Delete node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testDeleteNode() throws BaseXException {
    new XQuery("delete node //b").execute(CTX);
    System.out.println(new XQuery("//*[text() = 'test']").execute(CTX));
  }
}
