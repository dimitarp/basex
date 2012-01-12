package org.basex.io.random;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.util.BitBuffer;
import org.basex.util.Num;

/**
 * TODO: description missing.
 * TODO: decide how records with size > 4096 will be stored
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class BlockDataAccess {

  /**
   * Class representing a data block. Data blocks store actual records and some
   * meta-data about the block and the records.
   * <br/>
   * The on-disk format of each block is the following:
   * <ol>
   * <li>DATA AREA</li>
   * <p>The data area contains the data of each record. Records are stored
   * subsequently, but gaps may occur if a record is deleted. In order to reuse
   * the empty space, the records will be reordered.</p>
   *
   * <li>RECORD OFFSETS (SLOTS)</li>
   * <p>A list of offsets from the beginning of the data area. Each offset shows
   * the place where the record with the corresponding number is stored. If a
   * record is deleted, then its corresponding slot contains <code>NIL</code>.
   * <code>NIL</code>-slots will be reused.</p>
   *
   * <li>NUMBER OF SLOTS</li>
   * <p>Shows the number of slots, i.e. the size of the list with offsets.</p>
   *
   * <li>DATA AREA SIZE(IN BYTES)</li>
   * <p>Shows where the data area ends.</p>
   * </ol>
   *
   * <p>The meta-data are stored at the end of the block, so that the slot list
   * can grow without the need to shift the data area.</p>
   */
  private final class DataBlock {
    /** Size of an offset within a block in bits. */
    private static final int OFFSET_SIZE = IO.BLOCKPOWER;
    /** Maximal number of records in a data block. */
    private static final int MAX_RECORDS = IO.BLOCKSIZE >>> 1;
    /** Empty slot marker. */
    private static final int NIL = (int) SLOTMASK;

    /** Block id number. */
    private long id;
    /** Free space in bytes. */
    private int free;
    /** Dirty flag. */
    private boolean dirty;

    /** Bit buffer used to decode block meta-data. */
    private final BitBuffer buffer;

    // fields stored in the block:
    /** Data area size (in bytes). */
    private int size;
    /** Number of slots. */
    private int num;
    /** Slots with record offsets in the data area. */
    private final int[] slots = new int[MAX_RECORDS];

    /** Constructor. */
    public DataBlock() {
      buffer = new BitBuffer(BLOCKSIZEBITS);
    }

    /** Write the meta data to the current block. */
    private void writeMetaData() {
      int pos = BLOCKSIZEBITS - 1;

      // DATA AREA SIZE is stored at the end of each block
      buffer.write(pos, OFFSET_SIZE, size);
      pos -= OFFSET_SIZE;

      // NUMBER OF SLOTS is stored next
      buffer.write(pos, 10, num);
      pos -= 10;

      // SLOTS are stored next
      for(int i = 0; i < num; ++i) {
        buffer.write(pos, OFFSET_SIZE, slots[i]);
        pos -= OFFSET_SIZE;
      }

      buffer.serialize(da.buffer(false).data);
      dirty = false;
    }

    /** Read the meta data from the current block. */
    private void readMetaData() {
      // TODO: meta-data is small => don't copy the whole block!
      buffer.init(da.buffer(false).data);

      int pos = BLOCKSIZEBITS - 1;

      // DATA AREA SIZE is stored at the end of each block
      size = (int) buffer.read(pos, OFFSET_SIZE);
      pos -= OFFSET_SIZE;

      // NUMBER OF SLOTS is stored next
      num = (int) buffer.read(pos, 10);
      pos -= 10;

      // SLOTS are stored next
      for(int i = 0; i < num; ++i) {
        slots[i] = (int) buffer.read(pos, OFFSET_SIZE);
        pos -= OFFSET_SIZE;
      }

      free = (pos >>> 3) - size;
    }

    /**
     * Add a new record to the block.
     * @param d data record
     * @return record number within the block
     */
    public int insert(final byte[] d) {
      // write the record data
      final int off = allocate(Num.length(d.length) + d.length);
      da.off = off;
      da.writeToken(d);

      final int record = findEmptySlot();
      if(record == num) {
        // TODO: new slot will be allocated: the free space must be adjusted
        ++num;
      }

      slots[record] = off;

      dirty = true;

      return record;
    }

    /**
     * Delete a record from the block.
     * @param record record number within the block
     */
    public void delete(final int record) {
      if(record == num) {
        // the record from the last slot is deleted
        da.off = slots[record];
        int len = da.readNum();
        len += Num.length(len);
        // decrease size
        size -= len;
        // increase free
        free += len;
        // TODO: free the space occupied by the slot
        --num;
      }
      slots[record] = NIL;
      dirty = true;
    }

    /**
     * Retrieve the content of a record.
     * @param record record number within the block
     * @return data stored in the record
     */
    public byte[] select(final int record) {
      da.off = slots[record];
      return da.readToken();
    }

    /**
     * Go to the data block with the specified block id number.
     * @param b block id number
     */
    public void gotoBlock(final long b) {
      if(id == b) return;
      if(dirty) writeMetaData();
      da.gotoBlock(b);
      id = b;
      readMetaData();
    }

    /**
     * Find an empty slot; if none is available, create a new one at the end.
     * @return index of the empty slot
     */
    private int findEmptySlot() {
      for(int r = 0; r < num; ++r) if(slots[r] == NIL) return r;
      return num;
    }

    /**
     * Allocate a given number of bytes from the free space area.
     * @param l number of bytes
     * @return offset within the block where the bytes have been allocated
     */
    private int allocate(final int l) {
      // re-organize records, if not enough space
      if(l > free) compact();

      // allocate at the end of the data area
      final int off = size;
      size += l;
      free -= l;
      return off;
    }

    /** Compact records in a contiguous area. */
    private void compact() {
      int pos = 0;
      for(int i = 0; i < num; ++i) {
        // read the length of the record (size + data)
        da.off = slots[i];
        int len = da.readNum();
        len += Num.length(len);

        if(slots[i] > pos) {
          // there is unused space: shift the record (size + data) forwards
          da.off = slots[i];
          final byte[] record = da.readBytes(len);
          da.off = pos;
          da.writeBytes(record);
          slots[i] = pos;
        } else if(slots[i] < pos) {
          throw new RuntimeException("Not expected");
        }
        // next position should be right after the current record
        pos += len;
      }
      // adjust the free space value
      free += size - pos;
      // adjust the size value
      size = pos;
    }
  }

  /** Class representing a header block. */
  private final class HeaderBlock {
    /** Size of a block reference in bytes. */
    private static final int REFSIZE = 5;
    /** Number of data blocks per header. */
    private static final int BLOCKS = BLOCKSIZEBITS / (Byte.SIZE * REFSIZE + 1);

    /** Block index in the list of headers. */
    private int num;
    /** Block id number. */
    private long id;
    /** Dirty flag. */
    private boolean dirty;

    // data stored on disk
    /** Next header block; {@code 0} if the last one. */
    private long next;

    /** Id numbers of blocks managed by this header block. */
    public long[] blocks = new long[BLOCKS];
    /** Free space in each block. */
    private int[] space = new int[BLOCKS];

    /** Constructor. */
    public HeaderBlock() {
      // TODO Auto-generated constructor stub
    }

    /** Write the meta data to the current block. */
    public void writeMetaData() {
      // TODO
      dirty = false;
    }

    /** Read the meta data from the current block. */
    public void readMetaData() {
      // TODO
    }

    /**
     * Go to a header block.
     * @param n header block index
     */
    public void gotoHeaderBlock(final int n) {
      if(num == n) return;
      if(dirty) writeMetaData();
      // go to the first header, if n is smaller than the current header index
      if(num > n) {
        num = 0;
        next = 0L;
        da.gotoBlock(next);
        readMetaData();
      }
      // scan headers, until the required index
      while(num < n) {
        ++num;
        da.gotoBlock(next);
        readMetaData();
      }
    }

    /**
     * Find a data block, where a record with the given size can be stored.
     * @param s record size
     * @return block index
     */
    public int findBlock(final int s) {
      // TODO Auto-generated method stub
      dirty = true;
      return 0;
    }
  }

  /** Size of a block in bits. */
  private static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;
  /** Bit mask used to extract the slot number from a record id. */
  public static final long SLOTMASK = (1L << IO.BLOCKPOWER) - 1L;

  /** Underlying data file. */
  final BlockManagedDataAccess da;

  /** Current data block. */
  private final DataBlock data;
  /** Current header block. */
  private final HeaderBlock header;

  /**
   * Constructor; open a file for data access.
   * @param f file
   * @throws IOException I/O exception
   */
  public BlockDataAccess(final File f) throws IOException {
    da = new BlockManagedDataAccess(f);
    data = new DataBlock();
    header = new HeaderBlock();
  }

  /**
   * Insert a record.
   * @param d record data
   * @return record id
   */
  public long insert(final byte[] d) {
    final int b = header.findBlock(d.length);
    data.gotoBlock(header.blocks[b]);

    final long s = data.insert(d);
    return ((long) b << IO.BLOCKPOWER) & s;
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    gotoDataBlock(block(rid));
    data.delete(slot(rid));
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    gotoDataBlock(block(rid));
    return data.select(slot(rid));
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    da.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    da.close();
  }

  /**
   * Go to a data block.
   * @param n block index
   */
  private void gotoDataBlock(final int n) {
    header.gotoHeaderBlock(headerIndex(n));
    data.gotoBlock(header.blocks[n]);
  }

  /**
   * Extract the block index from a record id.
   * @param rid record id
   * @return block index
   */
  public static int block(final long rid) {
    return (int) (rid >>> IO.BLOCKPOWER);
  }

  /**
   * Extract the slot index from a record id.
   * @param rid record id
   * @return slot index
   */
  public static int slot(final long rid) {
    return (int) (rid & SLOTMASK);
  }

  /**
   * Get the index in the list of headers of the header block, which has the
   * address of the block with the given number.
   * @param b block number
   * @return header block index
   */
  public static int headerIndex(final int b) {
    return b / HeaderBlock.BLOCKS;
  }
}
