package org.basex.core.cmd;

import static org.basex.core.Text.*;
import static org.basex.data.DataText.*;
import java.io.IOException;
import org.basex.core.Context;
import org.basex.core.Command;
import org.basex.core.Prop;
import org.basex.core.User;
import org.basex.data.MetaData;
import org.basex.io.DataInput;
import org.basex.io.IO;
import org.basex.util.StringList;
import org.basex.util.Table;
import org.basex.util.TokenList;

/**
 * Evaluates the 'list' command and shows all available databases.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class List extends Command {
  /**
   * Default constructor.
   */
  public List() {
    super(STANDARD);
  }

  @Override
  protected boolean run() throws IOException {
    final Table table = new Table();
    table.desc = DATABASES;

    final boolean create = context.user.perm(User.CREATE);
    table.header.add(INFODBNAME);
    table.header.add(INFONDOCS);
    table.header.add(INFODBSIZE);
    if(create) table.header.add(INFOPATH);

    for(final String name : list(context)) {
      DataInput in = null;
      String file = null;
      long size = 0;
      int ndocs = 0;
      final MetaData meta = new MetaData(name, prop);
      try {
        in = new DataInput(meta.file(DATAINFO));
        meta.read(in);
        size = meta.dbsize();
        ndocs = meta.ndocs;
        if(context.perm(User.READ, meta)) file = meta.path.toString();
      } catch(final IOException ex) {
        file = INFODBERR;
      } finally {
        if(in != null) try { in.close(); } catch(final IOException ex) { }
      }
      if(file != null) {
        final TokenList tl = new TokenList();
        tl.add(name);
        tl.add(ndocs);
        tl.add(size);
        if(create) tl.add(file);
        table.contents.add(tl);
      }
    }
    table.sort();
    out.println(table.finish());
    return true;
  }

  /**
   * Returns a list of all databases.
   * @param ctx context reference
   * @return available databases
   */
  public static StringList list(final Context ctx) {
    // create database list
    final StringList db = new StringList();

    final IO dir = IO.get(ctx.prop.get(Prop.DBPATH));
    if(!dir.exists()) return db;

    for(final IO f : dir.children()) {
      if(f.name().startsWith(".")) continue;
      if(f.isDir()) db.add(f.name());
    }
    db.sort(false, true);
    return db;
  }
}
