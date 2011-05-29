package org.basex.test.data;

import static org.junit.Assert.*;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.DropDB;
import org.basex.core.cmd.XQuery;
import org.basex.data.DiskData;
import org.basex.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test index updates when using disk storage ({@link DiskData}).
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class DiskDataTest {
  /** Test database name. */
  private final String dbname = Util.name(DiskDataTest.class);
  /** Test XML document. */
  private final String xml = "<a><b>test</b><c/><f>test1</f><f>test3</f></a>";
  /** Database context. */
  protected final Context ctx = new Context();

  /**
   * Set up method; executed before each test; creates the database.
   * @throws BaseXException the database cannot be created
   */
  @Before
  public void setUp() throws BaseXException {
    new CreateDB(dbname, xml).execute(ctx);
  }

  /**
   * Clean up method; executed after each test; drops the database.
   * @throws BaseXException the database cannot be dropped
   */
  @After
  public void cleanUp() throws BaseXException {
    new DropDB(dbname).execute(ctx);
  }

  /**
   * Replace value update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testReplaceValue() throws BaseXException {
    new XQuery("replace value of node /a/b with 'test2'").execute(ctx);
    final String o = new XQuery("/a/b[text() = 'test']").execute(ctx);
    assertTrue("Old node found", o.length() == 0);
    final String n = new XQuery("/a/b[text() = 'test2']").execute(ctx);
    assertTrue("New node not found", n.length() > 0);
  }

  /**
   * Replace node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testReplaceNode() throws BaseXException {
    new XQuery("replace node /a/b with <d f='test2'/>").execute(ctx);
    final String o = new XQuery("/a/b").execute(ctx);
    assertTrue("Old node found", o.length() == 0);
    final String n = new XQuery("//d[@f = 'test2']").execute(ctx);
    assertTrue("New node not found", n.length() > 0);
  }

  /**
   * Insert node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testInsertDuplicateNode() throws BaseXException {
    new XQuery("insert node <d>test</d> as first into /a").execute(ctx);
    final String r = new XQuery("//d[text() = 'test']").execute(ctx);
    assertTrue("Node not found", r.length() > 0);
    final int c = Integer.parseInt(
        new XQuery("count(//*[text() = 'test'])").execute(ctx));
    assertTrue("Second node not found", c == 2);
  }

  /**
   * Insert node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testInsertNode() throws BaseXException {
    new XQuery("insert node <d>test2</d> as first into /a").execute(ctx);
    final String r = new XQuery("//d[text() = 'test2']").execute(ctx);
    assertTrue("Node not found", r.length() > 0);
    new XQuery("insert node <d>test2</d> as first into /a").execute(ctx);
    final int c = Integer.parseInt(
        new XQuery("count(//d[text() = 'test2'])").execute(ctx));
    assertTrue("Second node not found", c == 2);
  }

  /**
   * Delete node update test.
   * @throws BaseXException query exception
   */
  @Test
  public void testDeleteNode() throws BaseXException {
    new XQuery("delete node //b").execute(ctx);
    final String r = new XQuery("//*[text() = 'test']").execute(ctx);
    assertTrue("Node not deleted", r.length() == 0);
  }

  /**
   * Try to find non-existing node.
   * @throws BaseXException query exception
   */
  @Test
  public void testFindNonexistingNode() throws BaseXException {
    final String r = new XQuery("//*[text() = 'test0']").execute(ctx);
    assertTrue("Found non-existing node", r.length() == 0);
  }
}
