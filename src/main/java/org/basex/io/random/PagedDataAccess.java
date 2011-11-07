package org.basex.io.random;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.basex.io.IO;
import org.basex.util.BitArray;

/**
 * Access files with pages and page directory.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class PagedDataAccess extends DataAccess {
  /** Size of a page reference in bytes. */
  private static final int PAGEREFSIZE = (Long.SIZE + Short.SIZE) >>> 3;
  /** Number of page references in a directory page. */
  private static final int PAGEREFS = (IO.BLOCKSIZE - 2*(Long.SIZE >>> 3))/PAGEREFSIZE;
  /** Invalid reference value. */
  private static final long NIL = 0L;

  /**
   * Constructor, initializing the file reader.
   * @param f the file to be read
   * @throws IOException I/O Exception
   */
  public PagedDataAccess(final File f) throws IOException {
    super(f);
    if(length() == 0L) newDirPage();
  }

  @Override
  public void cursor(final long p) {
    // required page number
    int pageNumber = (int) p >>> IO.BLOCKPOWER;
    // required read position offset in the page
    long pageOffset = p % IO.BLOCKSIZE;

    // directory page number
    int dirPageNumber = pageNumber / PAGEREFS;
    long dirPageRef = dirPage(dirPageNumber);

    // required page reference offset within the directory page
    long pageRefOffset = (pageNumber % PAGEREFS + 2) << PAGEREFSIZEPOWER;
    // read page reference from directory page
    long pageRef = read8(dirPageRef + pageRefOffset);
    
    if(pageRef == NIL) pageRef = newPage();

    super.cursor(pageRef + pageOffset);
  }

  @Override
  protected void write(final int b) {
    
  }

  // private methods
  /** Initialize an empty file. */
  private void newDirPage() {
    final long end = length();
    cursor(end);

    final Buffer bm = buffer(false);
    // the following should be changed if NIL is changed!
    Arrays.fill(bm.data, (byte) 0);
    bm.dirty = true;

    length(end + IO.BLOCKSIZE);
  }

  private long newPage() {
    super.cursor(pageRef 
  }

  private long dirPage(final int n) {
    long pos = 0L;
    for(int i = 0; i < n; ++i) {
      long nextDirPage = read8(pos);
      if(nextDirPage == NIL) newDirPage();
      pos = nextDirPage;
    }
    return pos;
  }

  /**
   * Read an 8-byte value.
   * @return long value
   */
  private long read8(final long p) {
    super.cursor(p);
    return
        ((long) read() << 56) +
        ((long) read() << 48) +
        ((long) read() << 40) +
        ((long) read() << 32) +
        ((long) read() << 24) +
        (read() << 16) +
        (read() << 8) +
        read();
  }

  /**
   * Write an 8-byte value.
   * @param v long value
   */
  private void write8(final long v) {
    super.write((int) v >>> 56);
    super.write((int) v >>> 48);
    super.write((int) v >>> 40);
    super.write((int) v >>> 32);
    super.write((int) v >>> 24);
    super.write((int) v >>> 16);
    super.write((int) v >>>  8);
    super.write((int) v);
  }
}
