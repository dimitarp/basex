/*
 Copyright (C) 2014  Dimitar Popov

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 as
 published by the Free Software Foundation.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
package org.basex.query;

import org.basex.query.iter.Iter;
import org.basex.query.iter.ValueBuilder;
import org.basex.query.iter.ValueIter;
import org.basex.query.value.Value;
import org.basex.query.value.item.FuncItem;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.FElem;
import org.basex.query.value.seq.Seq;
import org.basex.query.value.type.AtomType;
import org.basex.query.value.type.SeqType;
import org.basex.util.InputInfo;
import org.basex.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.*;

/**
 * XQuery module doing stuff async.
 * @author Dimitar Popov
 */
public class AsyncModule extends QueryModule {

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Requires(Permission.NONE)
  @ContextDependent
  public Value eval(final FuncItem fun, final Value args) throws QueryException {
    final Future<Value> future = executorService.submit(new Callable<Value>() {
      @Override
      public Value call() throws Exception {
        return fun.invokeValue(queryContext, null, args);
      }
    });
    return new FutureValue(future);
  }

  @Requires(Permission.NONE)
  @ContextDependent
  public Iter forEach(final Seq partitions, final FuncItem fun) throws QueryException {
    final ValueIter iter = partitions.iter();
    ArrayList<Value> result = new ArrayList<>();
    for(Item it; (it = iter.next()) != null;) {
      result.add(new FutureValue(executorService.submit(new FunctionCall(fun, it, queryContext))));
    }
    final Iterator<Value> valueIterator = result.iterator();
    return new Iter() {
      Iter itemIterator = null;
      @Override
      public Item next() throws QueryException {
        Item nextItem;
        if (itemIterator != null && (nextItem = itemIterator.next()) != null) return nextItem;
        if (valueIterator.hasNext()) {
          itemIterator = valueIterator.next().iter();
          return itemIterator.next();
        }
        return null;
      }
    };
  }

  private static class FunctionCall implements Callable<Value> {
    private final FuncItem func;
    private final Value args;
    private final QueryContext queryContext;

    private FunctionCall(FuncItem func, Value args, QueryContext queryContext) {
      this.func = func;
      this.args = args;
      this.queryContext = queryContext;
    }

    @Override
    public Value call() throws Exception {
      return func.invokeValue(queryContext, null, args);
    }
  }

  private static class FutureValue extends Value {
    private final Future<Value> future;
    private FutureValue(Future<Value> f) {
      super(AtomType.ITEM);
      this.future = f;
    }

    private Value actual() throws QueryException {
      try {
        return future.get();
      } catch (ExecutionException e) {
        Throwable actual = e.getCause();
        if (actual instanceof QueryException) throw (QueryException) actual;
        Util.debug(e);
        throw Util.notExpected();
      } catch (Exception e) {
        Util.debug(e);
        throw Util.notExpected();
      }
    }

    private Value actualNoQueryException() {
      try {
        return actual();
      } catch (QueryException e) {
        Util.debug(e);
        throw Util.notExpected();
      }
    }

    @Override
    public ValueIter iter() {
      return actualNoQueryException().iter();
    }

    @Override
    public Value materialize(InputInfo ii) throws QueryException {
      return actual().materialize(ii);
    }

    @Override
    public Value atomValue(InputInfo ii) throws QueryException {
      return actual().atomValue(ii);
    }

    @Override
    public long atomSize() {
      return actualNoQueryException().atomSize();
    }

    @Override
    public Object toJava() throws QueryException {
      return actual().toJava();
    }

    @Override
    public int hash(InputInfo ii) throws QueryException {
      return 0;
    }

    @Override
    public int writeTo(Item[] arr, int index) {
      return actualNoQueryException().writeTo(arr, index);
    }

    @Override
    public Item itemAt(long pos) {
      return actualNoQueryException().itemAt(pos);
    }

    @Override
    public boolean homogeneous() {
      return actualNoQueryException().homogeneous();
    }

    @Override
    public Item item(QueryContext qc, InputInfo ii) throws QueryException {
      return actual().item(qc, ii);
    }

    @Override
    public Item atomItem(QueryContext qc, InputInfo ii) throws QueryException {
      return actual().atomItem(qc, ii);
    }

    @Override
    public Item ebv(QueryContext qc, InputInfo ii) throws QueryException {
      return actual().ebv(qc, ii);
    }

    @Override
    public Item test(QueryContext qc, InputInfo ii) throws QueryException {
      return actual().test(qc, ii);
    }

    @Override
    public long size() {
      return actualNoQueryException().size();
    }

    @Override
    public SeqType seqType() {
      return actualNoQueryException().seqType();
    }

    @Override
    public String toString() {
      return actualNoQueryException().toString();
    }

    @Override
    public void plan(FElem e) {
      actualNoQueryException().plan(e);
    }
  }
}
