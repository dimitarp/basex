package org.basex.gui.dialog;

import static org.basex.core.Text.*;
import static org.basex.data.DataText.*;
import java.awt.BorderLayout;
import java.awt.Font;
import java.io.IOException;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import org.basex.core.Context;
import org.basex.core.cmd.AlterDB;
import org.basex.core.cmd.Backup;
import org.basex.core.cmd.Close;
import org.basex.core.cmd.Copy;
import org.basex.core.cmd.DropDB;
import org.basex.core.cmd.InfoDB;
import org.basex.core.cmd.List;
import org.basex.core.cmd.Restore;
import org.basex.data.MetaData;
import org.basex.gui.GUI;
import org.basex.gui.GUIConstants;
import org.basex.gui.layout.BaseXBack;
import org.basex.gui.layout.BaseXButton;
import org.basex.gui.layout.BaseXLabel;
import org.basex.gui.layout.BaseXLayout;
import org.basex.gui.layout.BaseXListChooser;
import org.basex.gui.layout.BaseXText;
import org.basex.io.DataInput;
import org.basex.util.StringList;
import org.basex.util.Token;
import org.basex.util.Util;

/**
 * Open database dialog.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class DialogOpen extends Dialog {
  /** List of currently available databases. */
  private final BaseXListChooser choice;
  /** Information panel. */
  private final BaseXLabel doc;
  /** Information panel. */
  private final BaseXText detail;
  /** Buttons. */
  private final BaseXBack buttons;
  /** Rename button. */
  private final BaseXButton rename;
  /** Drop button. */
  private final BaseXButton drop;
  /** Open button. */
  private final BaseXButton open;
  /** Backup button. */
  private final BaseXButton backup;
  /** Restore button. */
  private final BaseXButton restore;
  /** Copy button. */
  private final BaseXButton copy;
  /** Manage flag. */
  private final boolean manage;
  /** Refresh. */
  private boolean refresh;

  /**
   * Default constructor.
   * @param main reference to the main window
   * @param m show manage dialog
   */
  public DialogOpen(final GUI main, final boolean m) {
    super(main, m ? MANAGETITLE : OPENTITLE);
    manage = m;
    // create database chooser
    final StringList db = List.list(main.context);

    choice = new BaseXListChooser(db.toArray(), this);
    set(choice, BorderLayout.CENTER);
    choice.setSize(130, 440);

    final BaseXBack info = new BaseXBack(new BorderLayout());
    info.setBorder(new CompoundBorder(new EtchedBorder(),
        new EmptyBorder(10, 10, 10, 10)));

    final Font f = choice.getFont();
    doc = new BaseXLabel(DIALOGINFO).border(0, 0, 5, 0);
    doc.setFont(f.deriveFont(f.getSize2D() + 7f));
    info.add(doc, BorderLayout.NORTH);

    detail = new BaseXText(false, this);
    detail.border(5, 5, 5, 5).setFont(f);

    BaseXLayout.setWidth(detail, 400);
    info.add(detail, BorderLayout.CENTER);

    final BaseXBack pp = new BaseXBack(new BorderLayout()).border(0, 12, 0, 0);
    pp.add(info, BorderLayout.CENTER);

    // create buttons
    final BaseXBack p = new BaseXBack(new BorderLayout());

    backup = new BaseXButton(BUTTONBACKUP, this);
    restore = new BaseXButton(BUTTONRESTORE, this);
    copy = new BaseXButton(BUTTONCOPY, this);
    rename = new BaseXButton(BUTTONRENAME, this);
    open = new BaseXButton(BUTTONOPEN, this);
    drop = new BaseXButton(BUTTONDROP, this);
    buttons = manage ?
        newButtons(this, backup, restore, copy, rename, drop, BUTTONOK) :
        newButtons(this, open, BUTTONCANCEL);
    p.add(buttons, BorderLayout.EAST);
    pp.add(p, BorderLayout.SOUTH);

    set(pp, BorderLayout.EAST);
    action(null);
    if(db.size() == 0) return;

    finish(null);
  }

  /**
   * Returns the database name.
   * @return database name
   */
  public String db() {
    return ok ? choice.getValue() : null;
  }

  /**
   * Tests if no databases have been found.
   * @return result of check
   */
  public boolean nodb() {
    return choice.getIndex() == -1;
  }

  @Override
  public void action(final Object cmp) {
    final Context ctx = gui.context;
    final String db = choice.getValue().trim();
    final String opendb = ctx.data != null ? ctx.data.meta.name : null;
    ok = true;
    if(refresh) choice.setData(List.list(ctx).toArray());
    refresh = false;
    if(cmp == open) {
      close();
    } else if(cmp == rename) {
      final DialogInput dr = new DialogInput(db, RENAMETITLE, gui, 1);
      if(!dr.ok() || dr.input().equals(db)) return;
      final AlterDB cmd = new AlterDB(db, dr.input());
      if(cmd.run(ctx)) {
        gui.notify.init();
      } else {
        Dialog.error(gui, cmd.info());
      }
      choice.setData(List.list(ctx).toArray());
      action(null);
    } else if(cmp == copy) {
      final DialogInput dc = new DialogInput(db, COPYTITLE, gui, 2);
      if(!dc.ok() || dc.input().equals(db)) return;
      setCursor(GUIConstants.CURSORWAIT);
      DialogProgress.execute(this, "", new Copy(db, dc.input()));
      setCursor(GUIConstants.CURSORARROW);
      refresh = true;
    } else if(cmp == drop) {
      if(db.isEmpty() || !Dialog.confirm(gui, Util.info(DROPCONF, db))) return;
      if(db.equals(opendb)) {
        new Close().run(ctx);
        gui.notify.init();
      }
      DropDB.drop(db, ctx.prop);
      choice.setData(List.list(ctx).toArray());
      choice.requestFocusInWindow();
      if(choice.getValue().isEmpty()) {
        doc.setText("");
        detail.setText(Token.EMPTY);
      }
      action(null);
    } else if(cmp == backup) {
      setCursor(GUIConstants.CURSORWAIT);
      DialogProgress.execute(this, "", new Backup(db));
      setCursor(GUIConstants.CURSORARROW);
    } else if(cmp == restore) {
      setCursor(GUIConstants.CURSORWAIT);
      DialogProgress.execute(this, "", new Restore(db));
      setCursor(GUIConstants.CURSORARROW);
      if(db.equals(opendb)) gui.notify.init();
    } else {
      // update components
      ok = ctx.prop.dbexists(db);
      enableOK(buttons, BUTTONDROP, ok);
      if(ok) {
        doc.setText(db);
        DataInput in = null;
        final MetaData meta = new MetaData(db, ctx.prop);
        try {
          in = new DataInput(meta.file(DATAINFO));
          meta.read(in);
          detail.setText(InfoDB.db(meta, true, true, true));
        } catch(final IOException ex) {
          detail.setText(Token.token(ex.getMessage()));
          ok = manage;
        } finally {
          if(in != null) try { in.close(); } catch(final IOException ex) { }
        }
      }
      enableOK(buttons, BUTTONOPEN, ok);
      enableOK(buttons, BUTTONRENAME, ok);
      enableOK(buttons, BUTTONBACKUP, ok);
      enableOK(buttons, BUTTONCOPY, ok);
      enableOK(buttons, BUTTONRESTORE, ok && Restore.list(db, ctx).size() != 0);
    }
  }

  @Override
  public void close() {
    if(ok || choice.getValue().isEmpty()) dispose();
  }
}
