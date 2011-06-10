package org.basex.server;

import java.io.InputStream;
import java.io.OutputStream;
import org.basex.core.BaseXException;
import org.basex.core.Command;
import org.basex.core.CommandParser;
import org.basex.core.Context;
import org.basex.core.cmd.Add;
import org.basex.core.cmd.CreateDB;
import org.basex.query.QueryException;
import org.basex.util.Util;

/**
 * This wrapper executes commands locally.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class LocalSession extends Session {
  /** Database context. */
  private final Context ctx;

  /**
   * Constructor.
   * @param context context
   */
  public LocalSession(final Context context) {
    this(context, null);
  }

  /**
   * Constructor.
   * @param context context
   * @param output output stream
   */
  public LocalSession(final Context context, final OutputStream output) {
    super(output);
    ctx = context;
  }

  @Override
  public void create(final String name, final InputStream input)
    throws BaseXException {
    info = CreateDB.create(name, input, ctx);
  }

  @Override
  public void add(final String name, final String target,
      final InputStream input) throws BaseXException {
    info = Add.add(name, target, input, ctx, null);
  }

  @Override
  public Query query(final String query) {
    Util.notimplemented();
    return null;
  }

  @Override
  public void close() {
  }

  @Override
  protected void execute(final String str, final OutputStream os)
      throws BaseXException {
    try {
      execute(new CommandParser(str, ctx).parseSingle(), os);
    } catch(final QueryException ex) {
      throw new BaseXException(ex);
    }
  }

  @Override
  protected void execute(final Command cmd, final OutputStream os)
      throws BaseXException {
    cmd.execute(ctx, os);
    info = cmd.info();
  }
}
