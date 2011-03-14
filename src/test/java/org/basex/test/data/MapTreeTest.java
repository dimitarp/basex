package org.basex.test.data;

import static org.junit.Assert.*;

import org.basex.data.MapTree;
import org.junit.Test;

/**
 * Various test cases.
 * @author Dimitar Popov
 */
public class MapTreeTest {

  /** Test 1. */
  @Test
  public void test1() {
    final MapTree m = new MapTree(2);
    m.delete(2, 2);
    m.insert(3, 2);

    assertEquals("Wrong PRE value of ID=1", 1, m.pre(1));
    assertEquals("Wrong PRE value of ID=3", 2, m.pre(3));
  }
}
