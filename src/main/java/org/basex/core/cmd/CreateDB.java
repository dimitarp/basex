package org.basex.core.cmd;

import static org.basex.core.Commands.*;
import static org.basex.core.Text.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.transform.sax.SAXSource;
import org.basex.build.BuildException;
import org.basex.build.Builder;
import org.basex.build.DiskBuilder;
import org.basex.build.MemBuilder;
import org.basex.build.Parser;
import org.basex.build.xml.DirParser;
import org.basex.build.xml.SAXWrapper;
import org.basex.core.BaseXException;
import org.basex.core.CommandBuilder;
import org.basex.core.Context;
import org.basex.core.Prop;
import org.basex.core.User;
import org.basex.core.Commands.Cmd;
import org.basex.core.Commands.CmdPerm;
import org.basex.data.Data;
import org.basex.index.IndexToken.IndexType;
import org.basex.index.FTBuilder;
import org.basex.index.ValueBuilder;
import org.basex.io.BufferInput;
import org.basex.io.IO;
import org.basex.io.IOContent;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.xml.sax.InputSource;

/**
 * Evaluates the 'create db' command and creates a new database.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class CreateDB extends ACreate {
  /**
   * Default constructor.
   * @param name name of database
   */
  public CreateDB(final String name) {
    this(name, null);
  }

  /**
   * Constructor, specifying an input.
   * @param name name of database
   * @param input input file path or XML string
   */
  public CreateDB(final String name, final String input) {
    super(name, input);
  }

  @Override
  protected boolean run() {
    final String name = args[0];
    final String input = args[1];
    if(input == null) return build(Parser.emptyParser(name), name);
    final IO io = IO.get(input);
    if(io instanceof IOContent) io.name(name + IO.XMLSUFFIX);
    return io.exists() ? build(new DirParser(io, prop), name) :
      error(FILEWHICH, io);
  }

  /**
   * Creates an empty database.
   * @param name name of the database
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static Data empty(final String name, final Context ctx)
      throws IOException {
    return xml(Parser.emptyParser(name), name, ctx);
  }

  /**
   * Creates a database for the specified file.
   * @param io input reference
   * @param name name of the database
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static synchronized Data xml(final IO io, final String name,
      final Context ctx) throws IOException {

    if(!ctx.user.perm(User.CREATE))
      throw new IOException(Util.info(PERMNO, CmdPerm.CREATE));
    if(!io.exists()) throw new BuildException(FILEWHICH, io);
    return xml(new DirParser(io, ctx.prop), name, ctx);
  }

  /**
   * Creates a database for the specified file.
   * @param name name of the database
   * @param input input stream
   * @param ctx database context
   * @return info string
   * @throws BaseXException database exception
   */
  public static synchronized String xml(final String name,
      final InputStream input, final Context ctx) throws BaseXException {

    final InputStream is = input instanceof BufferedInputStream ||
      input instanceof BufferInput ? input : new BufferedInputStream(input);
    final SAXSource sax = new SAXSource(new InputSource(is));
    return xml(name, new SAXWrapper(sax, ctx.prop), ctx);
  }

  /**
   * Creates a database for the specified file.
   * @param name name of the database
   * @param parser parser
   * @param ctx database context
   * @return info string
   * @throws BaseXException database exception
   */
  public static synchronized String xml(final String name,
      final Parser parser, final Context ctx) throws BaseXException {

    if(!ctx.user.perm(User.CREATE))
      throw new BaseXException(PERMNO, CmdPerm.CREATE);

    final Performance p = new Performance();
    ctx.register(true);
    try {
      ctx.openDB(xml(parser, name, ctx));
    } catch(final IOException ex) {
      throw new BaseXException(ex);
    } finally {
      ctx.unregister(true);
    }
    return Util.info(DBCREATED, name, p);
  }

  /**
   * Creates a database instance from the specified parser.
   * @param parser input parser
   * @param name name of the database
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static synchronized Data xml(final Parser parser, final String name,
      final Context ctx) throws IOException {

    // create main memory database instance
    if(ctx.prop.is(Prop.MAINMEM))
      return MemBuilder.build(parser, ctx.prop, name);

    // database is currently locked by another process
    if(ctx.pinned(name)) throw new IOException(Util.info(DBLOCKED, name));

    // build database and index structures
    final Builder builder = new DiskBuilder(parser, ctx.prop);
    try {
      final Data data = builder.build(name);
      if(data.meta.textindex) data.setIndex(IndexType.TEXT,
        new ValueBuilder(data, true).build());
      if(data.meta.attrindex) data.setIndex(IndexType.ATTRIBUTE,
        new ValueBuilder(data, false).build());
      if(data.meta.ftindex) data.setIndex(IndexType.FULLTEXT,
        FTBuilder.get(data).build());
      data.close();
    } finally {
      try {
        builder.close();
      } catch(final IOException exx) {
        Util.debug(exx);
      }
    }
    return Open.open(name, ctx);
  }

  /**
   * Creates a main memory database for the specified parser.
   * @param parser input parser
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static synchronized Data xml(final Parser parser, final Context ctx)
      throws IOException {

    if(!ctx.user.perm(User.CREATE))
      throw new IOException(Util.info(PERMNO, CmdPerm.CREATE));
    return MemBuilder.build(parser, ctx.prop);
  }

  /**
   * Creates a main memory database from the specified input reference.
   * @param io input reference
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static synchronized Data xml(final IO io, final Context ctx)
      throws IOException {
    if(!io.exists()) throw new BuildException(FILEWHICH, io.path());
    return xml(new DirParser(io, ctx.prop), ctx);
  }

  /**
   * Creates a main memory database from the specified SAX source.
   * @param sax sax source
   * @param ctx database context
   * @return database instance
   * @throws IOException I/O exception
   */
  public static synchronized Data xml(final SAXSource sax, final Context ctx)
      throws IOException {
    return xml(new SAXWrapper(sax, ctx.prop) , ctx);
  }

  @Override
  public void build(final CommandBuilder cb) {
    cb.init(Cmd.CREATE + " " + CmdCreate.DB).args();
  }
}
