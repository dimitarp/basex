package org.basex.query.func;

import static org.basex.query.QueryText.*;
import java.io.IOException;

import org.basex.io.serial.Serializer;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.expr.Arr;
import org.basex.query.expr.Expr;
import org.basex.query.item.Atm;
import org.basex.query.item.Item;
import org.basex.query.item.NodeType;
import org.basex.query.item.Str;
import org.basex.util.InputInfo;
import org.basex.util.Token;
import org.basex.util.TokenBuilder;

/**
 * Function call for built-in functions.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public abstract class FuncCall extends Arr {
  /** Function definition. */
  public Function def;

  /**
   * Constructor.
   * @param ii input info
   * @param fd function definition
   * @param args arguments
   */
  protected FuncCall(final InputInfo ii, final Function fd,
      final Expr... args) {
    super(ii, args);
    def = fd;
    type = def.ret;
  }

  @Override
  public final Expr comp(final QueryContext ctx) throws QueryException {
    // compile all arguments
    super.comp(ctx);
    // skip functions based on context or with non-values as arguments
    if(uses(Use.CTX) || !values()) return optPre(cmp(ctx), ctx);
    // pre-evaluate function
    return optPre(def.ret.zeroOrOne() ? item(ctx, input) : value(ctx), ctx);
  }

  /**
   * Performs function specific compilations.
   * @param ctx query context
   * @return evaluated item
   * @throws QueryException query exception
   */
  @SuppressWarnings("unused")
  public Expr cmp(final QueryContext ctx) throws QueryException {
    return this;
  }

  /**
   * Atomizes the specified item.
   * @param it input item
   * @return atomized item
   * @throws QueryException query exception
   */
  protected final Item atom(final Item it) throws QueryException {
    return it.node() ? it.type == NodeType.PI || it.type == NodeType.COM ?
        Str.get(it.atom(input)) : new Atm(it.atom(input)) : it;
  }

  @Override
  public final boolean isFun(final Function f) {
    return def == f;
  }

  @Override
  public final String desc() {
    return def.toString();
  }

  @Override
  public final void plan(final Serializer ser) throws IOException {
    ser.openElement(this, NAM, Token.token(def.desc));
    for(final Expr arg : expr) arg.plan(ser);
    ser.closeElement();
  }

  @Override
  public final String toString() {
    final String desc = def.toString();
    return new TokenBuilder().add(desc.substring(0,
        desc.indexOf('(') + 1)).addSep(expr, SEP).add(PAR2).toString();
  }
}
