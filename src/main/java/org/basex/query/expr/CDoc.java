package org.basex.query.expr;

import static org.basex.query.util.Err.XPATT;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.QueryText;
import org.basex.query.item.FDoc;
import org.basex.query.item.NodeType;
import org.basex.util.InputInfo;
import org.basex.util.Token;

/**
 * Document fragment.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class CDoc extends CFrag {
  /**
   * Constructor.
   * @param ii input info
   * @param e expression
   */
  public CDoc(final InputInfo ii, final Expr e) {
    super(ii, e);
  }

  @Override
  public FDoc item(final QueryContext ctx, final InputInfo ii)
      throws QueryException {

    final Constr c = new Constr(ii, ctx, expr);
    if(c.errAtt || c.atts.size() != 0) XPATT.thrw(ii);

    final FDoc doc = new FDoc(c.children, c.base);
    for(int n = 0; n < c.children.size(); ++n) c.children.get(n).parent(doc);
    return doc;
  }

  @Override
  public String desc() {
    return info(QueryText.DOCUMENT);
  }

  @Override
  public String toString() {
    return toString(Token.string(NodeType.DOC.nam()));
  }
}
