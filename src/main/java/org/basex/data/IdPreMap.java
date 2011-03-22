package org.basex.data;

import org.basex.util.IntList;

public class IdPreMap {

  final int baseid;
  final IntList pres;
  final IntList ops;

  public IdPreMap(final int i) {
    baseid = i;
    pres = new IntList(5);
    ops = new IntList(5);
  }

  public void insert(final int pre, final int id, final int c) {
    pres.add(pre);
    ops.add(c);
  }

  public void delete(final int pre, final int c) {

  }

  public int pre(final int id) {
    int pre = id;
    int c = 0;
    if(id > baseid) {
      c = id - baseid - 1;
      pre = pres.get(c++);
    }
    for(; c < pres.size(); c++) {
      if(pres.get(c) <= pre) pre += ops.get(c);
    }

    return pre;
  }
}
