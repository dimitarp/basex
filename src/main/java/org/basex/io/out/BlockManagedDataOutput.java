package org.basex.io.out;

import static org.basex.io.random.BlockManagedDataAccess.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.basex.io.IO;

/**
 * Implementation of {@link DataOutput} which however, adds additional blocks
 * for storing free block bit maps.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockManagedDataOutput extends DataOutput {
  /** Underlying file. */
  private final File file;

  /**
   * Constructor.
   * @param db database file.
   * @param bufs buffer size
   * @throws IOException I/O exception
   */
  public BlockManagedDataOutput(final File db, final int bufs)
      throws IOException {
    super(db, bufs);
    file = db;
    initHeader();
  }

  @Override
  public void write(final int b) throws IOException {
    if(size == SEGMENTSIZE) initHeader();
    super.write(b);
  }

  @Override
  public void close() throws IOException {
    super.close();
    adjustLastHeader();
  }

  /**
   * Initialize a header block. All bits are set to 1. At the end the header
   * must be adjusted.
   * @throws IOException I/O exception
   */
  private void initHeader() throws IOException {
    for(int i = 0; i < IO.BLOCKSIZE; ++i) os.write(BITMASK);
  }

  /**
   * Adjust the last header block. All previous header blocks should contain 1s,
   * i. e. all data blocks are occupied. This is not the case with the last
   * header, since the last segment may not be fully used.
   * @throws IOException I/O exception
   */
  private void adjustLastHeader() throws IOException {
    final long blocks = blocks(file.length());
    final long headers = segments(blocks);
    final long lastHeaderPos = position(headerBlock(headers - 1));

    final long lastDataBlock = blocks - headers - 1L;
    final long bit = dataBlockOffset(lastDataBlock);
    final long wordPos = lastHeaderPos + (bit >>> 3);
    final int word = BITMASK >>> (Byte.SIZE - modulo2(bit, Byte.SIZE) - 1);

    final RandomAccessFile f = new RandomAccessFile(file, "rw");
    try {
      f.seek(wordPos);
      f.write(word);
      // all subsequent bytes should be set to 0
      for(long p = modulo2(wordPos, IO.BLOCKSIZE) + 1L; p < IO.BLOCKSIZE; ++p) {
        f.seek(wordPos + p);
        f.write(0);
      }
    } finally {
      f.close();
    }
  }
}
