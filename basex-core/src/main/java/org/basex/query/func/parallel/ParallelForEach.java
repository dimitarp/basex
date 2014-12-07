package org.basex.query.func.parallel;

import org.basex.query.*;
import org.basex.query.func.*;
import org.basex.query.iter.*;
import org.basex.query.value.item.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.*;

/**
 * Function implementation.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class ParallelForEach extends StandardFunc {

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Override
  public Iter iter(final QueryContext qc) throws QueryException {
    final int threads = (int) toLong(exprs[1], qc);
    final ArrayList<Future<ValueBuilder>> values = new ArrayList<>(threads);

    // create and start threads
    for(int i = 0; i < threads; i++) {
      values.add(executorService.submit(new Callable<ValueBuilder>() {
        @Override
        public ValueBuilder call() throws Exception {
          try {
            final Iter ir = exprs[0].iter(qc);
            final ValueBuilder result = new ValueBuilder();
            for (Item it; (it = ir.next()) != null; ) result.add(it);
            return result;
          } catch(final QueryException ex) {
            throw new QueryRTException(ex);
          }
        }
      }));
    }
    final Iterator<Future<ValueBuilder>> valueIterator = values.iterator();

    return new Iter() {
      /** Currently returned values. */
      ValueBuilder vb;

      @Override
      public Item next() throws QueryException {
        do {
          if (vb == null) {
            if (valueIterator.hasNext()) vb = nextThreadResult();
            else return null;
          }
          final Item it = vb.next();
          if (it != null) return it;
          vb = null;
        } while (true);
      }

      private ValueBuilder nextThreadResult() throws QueryException {
        try {
          return valueIterator.next().get();
        } catch (Exception e) {
          throw new QueryException(e);
        }
      }
    };
  }
}
