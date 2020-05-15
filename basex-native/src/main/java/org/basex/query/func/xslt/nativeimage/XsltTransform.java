package org.basex.query.func.xslt.nativeimage;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;

/** XsltTransform substitute */
@TargetClass(org.basex.query.func.xslt.XsltTransform.class)
public final class XsltTransform {

  /**
   * com.sun.org.apache.xalan.internal.xsltc.compiler.XSLTC compiles stylesheets using BCEL to bytecode in runtime.
   */
  @Substitute
  final byte[] transform(final QueryContext qc) throws QueryException {
    throw new QueryException("XSLT transformations are currently not supported");
  }
}
