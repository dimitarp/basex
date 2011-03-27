package org.basex.query.expr;

import java.io.IOException;
import org.basex.data.Serializer;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.item.Empty;
import org.basex.query.item.Item;
import org.basex.query.item.SeqType;
import org.basex.query.item.Value;
import org.basex.query.iter.Iter;
import org.basex.query.iter.ItemCache;
import org.basex.query.path.AxisPath;
import org.basex.query.util.Var;
import org.basex.util.Array;
import org.basex.util.InputInfo;

/**
 * Filter expression.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public class Filter extends Preds {
  /** Expression. */
  Expr root;
  /** Offset flag. */
  private boolean off;

  /**
   * Constructor.
   * @param ii input info
   * @param r expression
   * @param p predicates
   */
  public Filter(final InputInfo ii, final Expr r, final Expr... p) {
    super(ii, p);
    root = r;
  }

  @Override
  public final Expr comp(final QueryContext ctx) throws QueryException {
    root = checkUp(root, ctx).comp(ctx);
    // convert filters to axis paths
    if(root instanceof AxisPath && !super.uses(Use.POS))
      return ((AxisPath) root).copy().addPreds(pred).comp(ctx);
    // return empty root
    if(root.empty()) return optPre(Empty.SEQ, ctx);

    final Value cv = ctx.value;
    ctx.value = null;
    final Expr e = super.comp(ctx);
    ctx.value = cv;
    if(e != this) return e;

    // no predicates.. return root
    if(pred.length == 0) return root;

    return comp2(ctx);
  }

  /**
   * Compiles the filter expression, excluding the root node.
   * @param ctx query context
   * @return compiled expression
   */
  public final Expr comp2(final QueryContext ctx) {
    // evaluate return type
    final SeqType t = root.type();
    type = SeqType.get(t.type, t.zeroOrOne() ? SeqType.Occ.ZO : SeqType.Occ.ZM);

    // no positional predicates.. use simple iterator
    if(!super.uses(Use.POS)) return new IterFilter(this);

    // one single position() or last() function specified:
    if(pred.length == 1 && (last || pos != null)) {
      // return single value
      if(root.value() && t.one() && (last || pos.min == 1 && pos.max == 1)) {
        return optPre(root, ctx);
      }
    }

    // iterator for simple positional predicate
    if(iterable()) return new IterPosFilter(this);

    // faster runtime evaluation of variable counters (array[$pos] ...)
    off = pred.length == 1 && pred[0].type().num() && !pred[0].uses(Use.CTX);
    return this;
  }

  @Override
  public Iter iter(final QueryContext ctx) throws QueryException {
    if(off) {
      // evaluate offset and create position expression
      final Item it = pred[0].ebv(ctx, input);
      final long l = it.itr(input);
      final Expr e = Pos.get(l, l, input);
      // don't accept fractional numbers
      if(l != it.dbl(input) || !(e instanceof Pos)) return Empty.ITER;
      pos = (Pos) e;
      return new IterPosFilter(this).iter(ctx);
    }

    final Iter iter = ctx.iter(root);
    final Value cv = ctx.value;
    final long cs = ctx.size;
    final long cp = ctx.pos;

    // cache results to support last() function
    final ItemCache ir = new ItemCache();
    for(Item i; (i = iter.next()) != null;) ir.add(i);

    // evaluate predicates
    for(final Expr p : pred) {
      final long is = ir.size();
      ctx.size = is;
      ctx.pos = 1;
      int c = 0;
      for(int s = 0; s < is; ++s) {
        ctx.value = ir.get(s);
        if(p.test(ctx, input) != null) ir.set(ir.get(s), c++);
        ctx.pos++;
      }
      ir.size(c);
    }
    ctx.value = cv;
    ctx.size = cs;
    ctx.pos = cp;
    return ir;
  }

  /**
   * Adds a predicate to the filter.
   * @param p predicate to be added
   * @return self reference
   */
  public final Filter addPred(final Expr p) {
    pred = Array.add(pred, p);
    return this;
  }

  @Override
  public final boolean uses(final Use u) {
    return root.uses(u) || u != Use.CTX && super.uses(u);
  }

  @Override
  public final int count(final Var v) {
    return root.count(v) + super.count(v);
  }

  @Override
  public final boolean removable(final Var v) {
    return root.removable(v) && super.removable(v);
  }

  @Override
  public final Expr remove(final Var v) {
    root = root.remove(v);
    return super.remove(v);
  }

  @Override
  public final void plan(final Serializer ser) throws IOException {
    ser.openElement(this);
    root.plan(ser);
    super.plan(ser);
    ser.closeElement();
  }

  @Override
  public final String toString() {
    return "(" + root + ")" + super.toString();
  }
}
