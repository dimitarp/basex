package org.basex.query.func;

import java.util.Arrays;
import java.util.Comparator;

import org.basex.query.QueryContext;
import org.basex.query.QueryError;
import org.basex.query.QueryException;
import org.basex.query.expr.DynFunCall;
import org.basex.query.expr.Expr;
import org.basex.query.expr.PartFunApp;
import org.basex.query.expr.VarRef;
import org.basex.query.item.AtomType;
import org.basex.query.item.Empty;
import org.basex.query.item.FunItem;
import org.basex.query.item.FunType;
import org.basex.query.item.Item;
import org.basex.query.item.Itr;
import org.basex.query.item.Value;
import static org.basex.query.util.Err.*;
import org.basex.query.iter.ItemCache;
import org.basex.query.iter.Iter;
import org.basex.query.util.Err;
import org.basex.query.util.Var;
import org.basex.util.InputInfo;

/**
 * Functions on functions.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
final class FNFunc extends Fun {
  /**
   * Constructor.
   * @param ii input info
   * @param f function definition
   * @param e arguments
   */
  protected FNFunc(final InputInfo ii, final FunDef f, final Expr... e) {
    super(ii, f, e);
  }

  @Override
  public Iter iter(final QueryContext ctx) throws QueryException {
    switch(def) {
      case MAP:       return map(ctx);
      case FILTER:    return filter(ctx);
      case MAPPAIRS:  return zip(ctx);
      case FOLDLEFT:  return foldLeft(ctx);
      case FOLDLEFT1: return foldLeft1(ctx);
      case FOLDRIGHT: return foldRight(ctx);
      case SORTWITH:  return sortWith(ctx);
      case HOFID:     return expr[0].iter(ctx);
      case CONST:     return expr[0].iter(ctx);
      case UNTIL:     return until(ctx);
      default:
           return super.iter(ctx);
    }
  }

  @Override
  public Item item(final QueryContext ctx, final InputInfo ii)
      throws QueryException {
    switch(def) {
      case FUNCARITY: return Itr.get(getFun(0, FunType.ANY, ctx).arity());
      case FUNCNAME:  return getFun(0, FunType.ANY, ctx).fName();
      case PARTAPP:   return partApp(ctx, ii);
      case HOFID:     return expr[0].item(ctx, ii);
      case CONST:     return expr[0].item(ctx, ii);
      default:
        return super.item(ctx, ii);
    }
  }

  /**
   * Partially applies the function to one argument.
   * @param ctx query context
   * @param ii input info
   * @return function item
   * @throws QueryException query exception
   */
  private FunItem partApp(final QueryContext ctx, final InputInfo ii)
      throws QueryException {
    final FunItem f = getFun(0, FunType.ANY, ctx);
    final Value v = expr[1].value(ctx);
    final long pos = expr.length == 2 ? 0 : checkItr(expr[2], ctx) - 1;

    final int arity = f.arity();
    if(pos < 0 || pos >= arity) INVPOS.thrw(ii, f.name(), pos + 1);

    final FunType ft = (FunType) f.type;
    final Var[] vars = new Var[arity - 1];
    final Expr[] vals = new Expr[arity];
    vals[(int) pos] = v;
    for(int i = 0, j = 0; i < arity - 1; i++, j++) {
      if(i == pos) j++;
      vars[i] = ctx.uniqueVar(ii, ft.args[j]);
      vals[j] = new VarRef(ii, vars[i]);
    }

    return (FunItem) new PartFunApp(ii,
        new DynFunCall(ii, f, vals), vars).comp(ctx);
  }

  /**
   * Maps a function onto a sequence of items.
   * @param ctx context
   * @return sequence of results
   * @throws QueryException exception
   */
  private Iter map(final QueryContext ctx) throws QueryException {
    final FunItem f = withArity(0, 1, ctx);
    final Iter xs = expr[1].iter(ctx);
    return new Iter() {

      /** Results. */
      Iter ys = Empty.ITER;

      @Override
      public Item next() throws QueryException {
        do {
          final Item it = ys.next();
          if(it != null) return it;
          final Item x = xs.next();
          if(x == null) return null;
          ys = f.invIter(ctx, input, x);
        } while(true);
      }
    };
  }

  /**
   * Filters the given sequence with the given predicate.
   * @param ctx query context
   * @return filtered sequence
   * @throws QueryException query exception
   */
  private Iter filter(final QueryContext ctx) throws QueryException {
    final FunItem f = withArity(0, 1, ctx);
    final Iter xs = expr[1].iter(ctx);
    return new Iter() {
      @Override
      public Item next() throws QueryException {
        do {
          final Item it = xs.next();
          if(it == null) return null;
          final Item b = f.invItem(ctx, input, it);
          if(checkType(b, AtomType.BLN).bool(input)) return it;
        } while(true);
      }
    };
  }

  /**
   * Zips two sequences with the given zipper function.
   * @param ctx query context
   * @return sequence of results
   * @throws QueryException query exception
   */
  private Iter zip(final QueryContext ctx) throws QueryException {
    final FunItem zipper = withArity(0, 2, ctx);
    final Iter xs = expr[1].iter(ctx);
    final Iter ys = expr[2].iter(ctx);
    return new Iter() {

      /** Results. */
      Iter zs = Empty.ITER;

      @Override
      public Item next() throws QueryException {
        do {
          final Item it = zs.next();
          if(it != null) return it;
          final Item x = xs.next(), y = ys.next();
          if(x == null || y == null) return null;
          zs = zipper.invIter(ctx, input, x, y);
        } while(true);
      }
    };
  }

  /**
   * Folds a sequence into a return value, starting from the left.
   * @param ctx query context
   * @return resulting sequence
   * @throws QueryException query exception
   */
  private Iter foldLeft(final QueryContext ctx) throws QueryException {
    final FunItem f = withArity(0, 2, ctx);
    final Iter xs = expr[2].iter(ctx);

    Iter res = expr[1].iter(ctx);
    for(Item x; (x = xs.next()) != null;)
      res = f.invIter(ctx, input, res.finish(), x);

    return res;
  }

  /**
   * Folds a sequence into a return value, starting from the left and using the
   * leftmost item as start value.
   * @param ctx query context
   * @return resulting sequence
   * @throws QueryException query exception
   */
  private Iter foldLeft1(final QueryContext ctx) throws QueryException {
    final FunItem f = withArity(0, 2, ctx);
    final Iter xs = expr[1].iter(ctx);

    Iter res = checkEmpty(xs.next()).iter();
    for(Item x; (x = xs.next()) != null;)
      res = f.invIter(ctx, input, res.finish(), x);

    return res;
  }

  /**
   * Folds a sequence into a return value, starting from the left.
   * @param ctx query context
   * @return resulting sequence
   * @throws QueryException query exception
   */
  private Iter foldRight(final QueryContext ctx) throws QueryException {
    final FunItem f = withArity(0, 2, ctx);
    Iter res = expr[1].iter(ctx);

    final ItemCache xs = ItemCache.get(expr[2].iter(ctx));
    for(int i = (int) xs.size(); i-- != 0;)
      res = f.invIter(ctx, input, xs.get(i), res.finish());

    return res;
  }

  /**
   * Sorts the input sequence according to the given relation.
   * @param ctx query context
   * @return sorted sequence
   * @throws QueryException query exception
   */
  private Iter sortWith(final QueryContext ctx) throws QueryException {
    final FunItem lt = withArity(0, 2, ctx);
    final ItemCache items = ItemCache.get(expr[1].iter(ctx));
    try {
      Arrays.sort(items.item, 0, (int) items.size(), new Comparator<Item>(){
        @Override
        public int compare(final Item it1, final Item it2) {
          try {
            return checkType(lt.invItem(ctx, input, it1, it2),
                AtomType.BLN).bool(input) ? -1 : 1;
          } catch(final QueryException qe) {
            throw new QueryError(qe);
          }
        }
      });
    } catch(final QueryError err) {
      throw err.wrapped();
    }
    return items;
  }

  /**
   * Applies a function to a start value until the given predicate holds.
   * @param ctx query context
   * @return accepted value
   * @throws QueryException exception
   */
  private Iter until(final QueryContext ctx) throws QueryException {
    final FunItem pred = withArity(0, 1, ctx);
    final FunItem fun = withArity(1, 1, ctx);
    Value v = expr[2].value(ctx);
    while(!checkType(pred.invItem(ctx, input, v), AtomType.BLN).bool(input)) {
      v = fun.invIter(ctx, input, v).finish();
    }
    return v.iter();
  }

  /**
   * Checks the type of the given function item.
   * @param p position
   * @param t type
   * @param ctx context
   * @return function item
   * @throws QueryException query exception
   */
  private FunItem getFun(final int p, final FunType t, final QueryContext ctx)
      throws QueryException {
    return (FunItem) checkType(checkItem(expr[p], ctx), t);
  }

  /**
   * Casts and checks the function item for its arity.
   * @param p position of the function
   * @param a arity
   * @param ctx query context
   * @return function item
   * @throws QueryException query exception
   */
  private FunItem withArity(final int p, final int a, final QueryContext ctx)
      throws QueryException {
    final Item f = checkItem(expr[p], ctx);
    if(!f.func() || ((FunItem) f).vars().length != a)
      Err.type(this, FunType.arity(a), f);

    return (FunItem) f;
  }

  @Override
  public boolean uses(final Use u) {
    return u == Use.X30 || super.uses(u);
  }

  @Override
  public Expr cmp(final QueryContext ctx) throws QueryException {
    return super.cmp(ctx);
  }
}
