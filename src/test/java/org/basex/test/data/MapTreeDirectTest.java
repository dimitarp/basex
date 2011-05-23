package org.basex.test.data;

import static org.junit.Assert.*;

import org.basex.data.MapTree;
import org.junit.Test;

/**
 * Various test cases.
 * @author Dimitar Popov
 */
public class MapTreeDirectTest {

  /** Special case 1. */
  @Test
  public void test1() {
    final MapTree m = new MapTree(2);
    m.delete(2, 2);
    m.insert(3, 2);

    assertPre(m, 1, 1);
    assertPre(m, 3, 2);
  }

  /** Special case 2. */
  @Test
  public void test2() {
    final MapTree m = new MapTree(2);
    m.delete(2, 2);
    m.insert(3, 2);

    m.insert(4, 2);

    m.delete(4, 2);
    m.insert(5, 2);

    m.insert(6, 2);

    m.delete(6, 2);
    m.insert(7, 2);

    assertPre(m, 1, 1);
    assertPre(m, 7, 2);
    assertPre(m, 5, 3);
    assertPre(m, 3, 4);
  }

  /** Special case 3. */
  @Test
  public void test3() {
    final MapTree m = new MapTree(2);
    m.delete(2, 2);
    m.insert(3, 2);

    m.delete(3, 2);
    m.insert(4, 2);

    m.delete(4, 2);
    m.insert(5, 2);

    m.delete(5, 2);
    m.insert(6, 2);

    m.delete(6, 2);
    m.insert(7, 2);

    assertPre(m, 1, 1);
    assertPre(m, 7, 2);
  }

  /**
   * Compare the actual PRE value with the expected.
   * @param m ID -> PRE mapping
   * @param id id to check
   * @param pre expected PRE value
   */
  private void assertPre(final MapTree m, final int id, final int pre) {
    assertEquals("Wrong PRE value for ID=" + id, pre, m.pre(id));
  }
}
