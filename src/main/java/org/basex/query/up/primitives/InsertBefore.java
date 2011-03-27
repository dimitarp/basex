package org.basex.query.up.primitives;

import org.basex.data.Data;
import org.basex.query.item.DBNode;
import org.basex.query.item.ANode;
import org.basex.query.iter.NodeCache;
import org.basex.util.InputInfo;
import org.basex.util.Util;

/**
 * Insert before primitive.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Lukas Kircher
 */
public final class InsertBefore extends NodeCopy {
  /**
   * Constructor.
   * @param ii input info
   * @param n target node
   * @param copy copy of nodes to be inserted
   */
  public InsertBefore(final InputInfo ii, final ANode n, final NodeCache copy) {
    super(ii, n, copy);
  }

  @Override
  public int apply(final int add) {
    // source nodes may be empty, thus insert has no effect at all
    if(md != null) {
      final DBNode n = (DBNode) node;
      final Data d = n.data;
      final int pre = n.pre;
      d.insert(pre, d.parent(pre, d.kind(pre)), md);
      return md.meta.size;
    }
    return 0;
  }

  @Override
  public void merge(final Primitive p) {
    insert.add(((NodeCopy) p).insert.get(0));
  }

  @Override
  public PrimitiveType type() {
    return PrimitiveType.INSERTBEFORE;
  }

  @Override
  public String toString() {
    return Util.name(this) + "[" + node + ", " + insert + "]";
  }
}
