package org.basex.test.build;

import static org.junit.Assert.*;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.Add;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.DropDB;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.util.Token;
import org.basex.util.Util;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for creating databases and adding documents.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class CreateTest {
  /** Database context. */
  private static final Context CONTEXT = new Context();

  /** Test database name. */
  private static final String DBNAME = Util.name(CreateTest.class);
  /** Test document name. */
  private static final String DOCNAME = "t.xml";
  /** Target. */
  private static final String TARGET = "target/";

  /** Path to test file. */
  private static final String PATH = "etc/test/input.xml";
  /** Test folder. */
  private static final String FOLDER = "etc/test/dir";
  /** Test XML fragment. */
  private static final String FRAG = "<xml/>";

  /** First document in test folder. */
  private static final String FOLDERFILE;

  // finds the first document in the xml folder
  static {
    String file = "";
    for(final IOFile c : new IOFile(FOLDER).children()) {
      if(c.name().endsWith(IO.XMLSUFFIX)) {
        file = c.name();
        break;
      }
    }
    FOLDERFILE = file;
  }

  /** Test inputs (path, folder, fragment). */
  private static final String[] INPUTS = { PATH, FRAG, FOLDER };
  /** Names of test inputs (path, folder, fragment). */
  private static final String[] NAMES = {
    PATH.replaceAll(".*/", ""), DBNAME + IO.XMLSUFFIX, FOLDERFILE
  };

  /**
   * Drops the initial collection.
   * @throws BaseXException exception
   */
  @After
  public void tearDown() throws BaseXException {
    new DropDB(DBNAME).execute(CONTEXT);
  }

  /**
   * CREATE DB {DBNAME}.
   * @throws BaseXException exception
   */
  @Test
  public void createDB() throws BaseXException {
    new CreateDB(DBNAME).execute(CONTEXT);
    // check if database name equals argument of create command
    assertEquals(dbName(), DBNAME);
  }

  /**
   * CREATE DB {DBNAME} {INPUT[]}.
   * @throws BaseXException exception
   */
  @Test
  public void createDBWithInput() throws BaseXException {
    for(int i = 0; i < INPUTS.length; ++i) {
      new CreateDB(DBNAME, INPUTS[i]).execute(CONTEXT);
      // check name of database
      assertEquals(DBNAME, dbName());
      // check name of document
      assertEquals(NAMES[i], docName());
    }
  }

  /**
   * CREATE DB {DBNAME}; ADD {INPUT[]}.
   * @throws BaseXException exception
   */
  @Test
  public void createDBandAdd() throws BaseXException {
    for(int i = 0; i < INPUTS.length; ++i) {
      new CreateDB(DBNAME).execute(CONTEXT);
      new Add(INPUTS[i]).execute(CONTEXT);
      // check name of document
      assertEquals(NAMES[i], docName());
    }
  }

  /**
   * CREATE DB {DBNAME}; ADD AS {DOCNAME} {INPUT[]}.
   * @throws BaseXException exception
   */
  @Test
  public void createDBandAddAs() throws BaseXException {
    for(final String in : INPUTS) {
      new CreateDB(DBNAME).execute(CONTEXT);
      new Add(in, DOCNAME).execute(CONTEXT);
      // check name of document (first file in folder or specified name)
      assertEquals(in == FOLDER ? FOLDERFILE : DOCNAME, docName());
    }
  }

  /**
   * CREATE DB {DBNAME}; ADD TO {TARGET} {INPUT[]}.
   * @throws BaseXException exception
   */
  @Test
  public void createDBandAddTo() throws BaseXException {
    for(int i = 0; i < INPUTS.length; ++i) {
      new CreateDB(DBNAME).execute(CONTEXT);
      new Add(INPUTS[i], null, TARGET).execute(CONTEXT);
      // check name of document
      assertEquals(TARGET + NAMES[i], docName());
    }
  }

  /**
   * CREATE DB {DBNAME}; ADD AS {DOCNAME} TO {TARGET} {INPUT[]}.
   * @throws BaseXException exception
   */
  @Test
  public void createDBandAddAsTO() throws BaseXException {
    for(final String in : INPUTS) {
      new CreateDB(DBNAME).execute(CONTEXT);
      new Add(in, DOCNAME, TARGET).execute(CONTEXT);
      // check name of document (first file in folder or specified name)
      assertEquals(TARGET + (in == FOLDER ? FOLDERFILE : DOCNAME), docName());
    }
  }

  /**
   * Returns the name of the database.
   * @return database name
   */
  private String dbName() {
    return CONTEXT.data.meta.name;
  }

  /**
   * Returns the name of the first document in the database.
   * @return first document name
   */
  private String docName() {
    return Token.string(CONTEXT.data.text(CONTEXT.current.list[0], true));
  }
}
