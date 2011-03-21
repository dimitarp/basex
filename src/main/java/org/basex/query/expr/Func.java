package org.basex.query.expr;

import static org.basex.query.util.Err.*;
import static org.basex.query.QueryTokens.*;
import java.io.IOException;
import org.basex.data.Serializer;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.item.QNm;
import org.basex.query.item.SeqType;
import org.basex.query.item.Value;
import org.basex.query.iter.Iter;
import org.basex.query.util.Var;
import org.basex.util.InputInfo;
import org.basex.util.Token;
import org.basex.util.TokenBuilder;

/**
 * User-defined function.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public class Func extends Single {
  /** Function name. */
  public final QNm name;
  /** Return type. */
  public SeqType ret;
  /** Arguments. */
  public final Var[] args;
  /** Declaration flag. */
  public final boolean declared;
  /** Updating flag. */
  public boolean updating;

  /**
   * Function constructor.
   * @param ii input info
   * @param n function name
   * @param a arguments
   * @param r return type
   * @param d declaration flag
   */
  public Func(final InputInfo ii, final QNm n, final Var[] a, final SeqType r,
      final boolean d) {
    super(ii, null);
    name = n;
    ret = r;
    args = a;
    declared = d;
  }

  /**
   * Checks the function for updating behavior.
   * @throws QueryException query exception
   */
  public void check() throws QueryException {
    if(!declared || expr == null) FUNCUNKNOWN.thrw(input, name.atom());

    final boolean u = expr.uses(Use.UPD);
    if(updating) {
      // updating function
      if(ret != null) UPFUNCTYPE.thrw(input);
      if(!u && !expr.vacuous()) UPEXPECTF.thrw(input);
    } else if(u) {
      // uses updates, but is not declared as such
      UPNOT.thrw(input, desc());
    }
  }

  @Override
  public Expr comp(final QueryContext ctx) throws QueryException {
    final int s = ctx.vars.size();
    for(final Var v : args) ctx.vars.add(v);
    expr = expr.comp(ctx);
    ctx.vars.reset(s);

    // returned expression will be ignored
    return this;
  }

  @Override
  public Iter iter(final QueryContext ctx) throws QueryException {
    // evaluate function and reset variable scope
    final Value cv = ctx.value;
    ctx.value = null;
    final Value v = expr.value(ctx);
    ctx.value = cv;
    return (ret != null ? ret.cast(v, ctx, input) : v).iter();
  }

  @Override
  public void plan(final Serializer ser) throws IOException {
    ser.openElement(this);
    ser.attribute(NAM, name.atom());
    for(int i = 0; i < args.length; ++i) {
      ser.attribute(Token.token(ARG + i), args[i].name.atom());
    }
    expr.plan(ser);
    ser.closeElement();
  }

  @Override
  public String toString() {
    final TokenBuilder tb = new TokenBuilder(name.atom());
    tb.add(PAR1).addSep(args, SEP).add(PAR2);
    if(ret != null) tb.add(' ' + AS + ' ' + ret);
    if(expr != null) tb.add(" { " + expr + " }; ");
    return tb.toString();
  }
}
