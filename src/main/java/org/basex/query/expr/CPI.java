package org.basex.query.expr;

import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.QueryTokens;
import org.basex.query.item.FPI;
import org.basex.query.item.Item;
import org.basex.query.item.NodeType;
import org.basex.query.item.QNm;
import org.basex.query.item.AtomType;
import org.basex.query.iter.Iter;
import org.basex.util.InputInfo;
import org.basex.util.Token;
import org.basex.util.TokenBuilder;
import org.basex.util.XMLToken;

/**
 * PI fragment.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class CPI extends CFrag {
  /**
   * Constructor.
   * @param ii input info
   * @param n name
   * @param v value
   */
  public CPI(final InputInfo ii, final Expr n, final Expr v) {
    super(ii, n, v);
  }

  @Override
  public FPI item(final QueryContext ctx, final InputInfo ii)
      throws QueryException {
    final Item it = checkItem(expr[0], ctx);
    if(!it.unt() && !it.str() && it.type != AtomType.QNM)
      CPIWRONG.thrw(input, it.type, it);

    final byte[] nm = trim(it.atom(ii));
    if(eq(lc(nm), XML)) CPIXML.thrw(input, nm);
    if(!XMLToken.isNCName(nm)) CPIINVAL.thrw(input, nm);

    final Iter iter = ctx.iter(expr[1]);
    final TokenBuilder tb = new TokenBuilder();
    CAttr.add(tb, iter, ii);
    byte[] v = tb.finish();

    int i = -1;
    while(++i != v.length && v[i] >= 0 && v[i] <= ' ');
    v = substring(v, i);
    return new FPI(new QNm(nm), FPI.parse(v, input), null);
  }

  @Override
  public String desc() {
    return info(QueryTokens.PI);
  }

  @Override
  public String toString() {
    return toString(Token.string(NodeType.PI.nam));
  }
}
