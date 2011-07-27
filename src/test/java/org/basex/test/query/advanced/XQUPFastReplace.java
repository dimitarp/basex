package org.basex.test.query.advanced;

import static org.junit.Assert.*;

import org.basex.core.BaseXException;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.DropDB;
import org.basex.query.QueryException;
import org.basex.util.Util;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * Stress Testing the fast replace feature where blocks on disk are directly
 * overwritten.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Lukas Kircher
 */
public final class XQUPFastReplace extends AdvancedQueryTest {
  /** Test document. */
  public static final String DOC = "etc/test/xmark.xml";
  /** Test database name. */
  public static final String DBNAME = Util.name(XQUPFastReplace.class);

  /**
   * Creates the db based on xmark.xml.
   * @throws Exception exception
   */
  @Before
  public void setUp() throws Exception {
    new CreateDB(DBNAME, DOC).execute(CONTEXT);
    query("let $items := /site/regions//item " +
      "for $i in 1 to 10 " +
      "return (insert node $items into /site/regions, " +
      "insert node $items before /site/regions, " +
      "insert node $items after /site/closed_auctions)");
  }

  /**
   * Replaces blocks of equal size distributed over the document.
   * @throws QueryException query exception
   */
  @Test
  public void replaceEqualBlocks() throws QueryException {
    query("for $i in //item/location/text() " +
      "return replace node $i with $i");
    query("count(//item)", "186");
  }

  /**
   * Replaces blocks of equal size distributed over the document.
   * @throws QueryException query exception
   */
  @Test
  public void replaceEqualBlocks2() throws QueryException {
    query("for $i in //item return replace node $i with $i");
    query("count(//item)", "186");
  }

  /**
   * Replaces blocks where the new subtree is smaller than the old one. Find
   * the smallest //item node in the database and replace each //item with
   * this.
   * @throws QueryException query exception
   */
  @Test
  public void replaceWithSmallerTree() throws QueryException {
    final String id =
      query("let $newitem := (let $c := min(for $i in //item " +
        "return count($i/descendant-or-self::node())) " +
        "return for $i in //item where " +
        "(count($i/descendant-or-self::node()) = $c) " +
        "return $i)[1] return $newitem/@id/data()");
    final String count1 = query("count(//item)");

    query("for $i in //item return replace node $i " +
        "with (//item[@id='" + id + "'])[1]");
    final String count2 =  query("count(//item[@id='" + id + "'])");

    assertEquals(count1, count2);
  }

  /**
   * Replaces blocks where the new subtree is bigger than the old one. Find
   * the biggest //item node in the database and replace each //item with
   * this.
   * @throws QueryException query exception
   */
  @Test
  public void replaceWithBiggerTree() throws QueryException {
    query("let $newitem := (let $c := max(for $i in //item " +
      "return count($i/descendant-or-self::node())) " +
      "return for $i in //item where " +
      "(count($i/descendant-or-self::node()) = $c) " +
      "return $i)[1] return for $i in //item " +
      "return replace node $i with $newitem");
    query("count(//item)", "186");
  }

  /**
   * Replaces blocks where the new subtree is bigger than the old one. Find
   * the biggest //item node in the database and replace the last item in the
   * database with this.
   * @throws QueryException query exception
   */
  @Test
  public void replaceSingleWithBiggerTree() throws QueryException {
    query("let $newitem := (let $c := max(for $i in //item " +
      "return count($i/descendant-or-self::node())) " +
      "return for $i in //item where " +
      "(count($i/descendant-or-self::node()) = $c) " +
      "return $i)[1] return replace node (//item)[last()] with $newitem");
    query("count(//item)", "186");
  }

  /**
   * Replaces blocks where the new subtree is bigger than the old one. Find
   * the biggest //item node in the database and replace the last item in the
   * database with this.
   * @throws QueryException query exception
   */
  @Test
  public void replaceSingleWithSmallerTree() throws QueryException {
    final String id =
      query("let $newitem := (let $c := min(for $i in //item " +
        "return count($i/descendant-or-self::node())) " +
        "return for $i in //item where " +
        "(count($i/descendant-or-self::node()) = $c) " +
        "return $i)[1] return $newitem/@id/data()");
    query("replace node (//item)[last()] with (//item[@id='" + id + "'])[1]");
    query("count(//item)", "186");
  }

  /**
   * Replaces a single attribute with two attributes. Checks for correct
   * updating of the parent's attribute size.
   * @throws QueryException query exception
   */
  @Test
  public void replaceAttributes() throws QueryException {
    query("replace node (//item)[1]/attribute() with " +
      "(attribute att1 {'0'}, attribute att2 {'1'})");
    query("(//item)[1]/attribute()", " att1=\"0\" att2=\"1\"");
  }

  /**
   * Replaces a single attribute with two attributes for each item. Checks for
   * correct updating of the parent's attribute size.
   * @throws QueryException query exception
   */
  @Test
  public void replaceAttributes2() throws QueryException {
    query("for $i in //item return replace node $i/attribute() with " +
    "(attribute att1 {'0'}, attribute att2 {'1'})");
    query("//item/attribute()");
  }

  /**
   * Deletes the test db.
   * @throws BaseXException database exception
   */
  @AfterClass
  public static void end() throws BaseXException {
    new DropDB(DBNAME).execute(CONTEXT);
    CONTEXT.close();
  }
}