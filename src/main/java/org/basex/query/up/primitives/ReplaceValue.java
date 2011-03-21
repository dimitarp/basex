package org.basex.query.up.primitives;

import static org.basex.query.util.Err.*;
import org.basex.data.Data;
import org.basex.query.QueryException;
import org.basex.query.item.DBNode;
import org.basex.query.item.ANode;
import org.basex.query.item.QNm;
import org.basex.util.InputInfo;
import org.basex.util.Util;

/**
 * Replace value primitive.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Lukas Kircher
 */
public final class ReplaceValue extends NewValue {
  /**
   * Constructor.
   * @param ii input info
   * @param n target node
   * @param newName new name
   */
  public ReplaceValue(final InputInfo ii, final ANode n, final QNm newName) {
    super(ii, n, newName);
  }

  @Override
  public void apply(final int add) {
    final DBNode n = (DBNode) node;
    final Data d = n.data;
    final int k = d.kind(n.pre);
    final byte[] nn = name.atom();

    if(k == Data.TEXT && nn.length == 0) {
      d.delete(n.pre);
    } else {
      d.replace(n.pre, k, nn);
    }
  }

  @Override
  public void merge(final UpdatePrimitive p) throws QueryException {
    UPMULTREPV.thrw(input, node);
  }

  @Override
  public PrimitiveType type() {
    return PrimitiveType.REPLACEVALUE;
  }

  @Override
  public String toString() {
    return Util.name(this) + "[" + node + ", " + name + "]";
  }
}
