package org.basex.query.expr;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.QueryText;
import org.basex.query.item.FComm;
import org.basex.query.item.Item;
import org.basex.query.item.NodeType;
import org.basex.query.iter.Iter;
import org.basex.util.InputInfo;
import org.basex.util.Token;
import org.basex.util.TokenBuilder;

/**
 * Comment fragment.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class CComm extends CFrag {
  /**
   * Constructor.
   * @param ii input info
   * @param c comment
   */
  public CComm(final InputInfo ii, final Expr c) {
    super(ii, c);
  }

  @Override
  public FComm item(final QueryContext ctx, final InputInfo ii)
      throws QueryException {
    final Iter iter = ctx.iter(expr[0]);

    final TokenBuilder tb = new TokenBuilder();
    boolean more = false;
    for(Item it; (it = iter.next()) != null;) {
      if(more) tb.add(' ');
      tb.add(it.atom(ii));
      more = true;
    }
    return new FComm(FComm.parse(tb.finish(), input), null);
  }

  @Override
  public String desc() {
    return info(QueryText.COMMENT);
  }

  @Override
  public String toString() {
    return toString(Token.string(NodeType.COM.nam()));
  }
}
