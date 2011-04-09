package org.basex.test.data;

import static org.junit.Assert.*;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.DropDB;
import org.basex.core.cmd.XQuery;
import org.basex.data.DiskData;
import org.basex.util.Util;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the ID -> PRE mapping facility.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Leo Woerteler
 * @author Dimitar Popov
 */
public final class IdPreMapTest {
  /** Database context. */
  private static Context context;

  /** Test document. */
  private static final String DOC = "<a><b><c/></b><d/></a>";

  /** Test queries. */
  private static final String[] QUERIES = new String[] {
      "insert node <e/> as first into //b",
      "insert node <f/> as first into //c", "delete //e",
      "insert node <e a='test'><f/><g/><h>text</h><i/></e> as first into /a",
      "delete //e", "replace node //d with <f r='t'></f>",
      "insert node <d><g/><h/><i/><j/><k/><l/><m/><n/></d> as first into //a"};

  /**
   * Tests if inserts are correctly reflected in the mapping.
   * @throws Exception exception
   */
  @Test
  public void insert() throws Exception {
    test(QUERIES);
  }

  /** Creates the database context. */
  @BeforeClass
  public static void start() {
    context = new Context();
  }

  /**
   * Creates all test databases.
   * @throws BaseXException database exception
   */
  @Before
  public void startTest() throws BaseXException {
    new CreateDB(Util.name(this), DOC).execute(context);
  }

  /**
   * Removes test databases and closes the database context.
   * @throws BaseXException database exception
   */
  @AfterClass
  public static void finish() throws BaseXException {
    new DropDB(Util.name(IdPreMapTest.class)).execute(context);
    context.close();
  }

  /**
   * Executes the given XQueries on the current DB and checks the mapping.
   * @param qs queries
   * @throws BaseXException database exception
   */
  private static void test(final String... qs) throws BaseXException {
    checkMapping(-1, "");
    for(int i = 0; i < 80; i++) {
      for(final String q : qs) {
        new XQuery(q).execute(context);
        checkMapping(i, q);
      }
    }
  }

  /**
   * Checks if for all PRE values existing in the document pre(id(PRE)) == PRE
   * holds.
   * @param i iteration
   * @param q query
   */
  private static void checkMapping(final int i, final String q) {
    for(int pre = 0; pre < context.data.meta.size; ++pre) {
      final int id = context.data.id(pre);
      assertEquals("Wrong PRE value for ID " + id + ":", pre,
          context.data.pre(id));
      assertEquals("Wrong PRE value for ID " + id + " in map ([" + i + "] " + q
          + "):", pre, ((DiskData) context.data).idmap.pre(id));
    }
  }
}
