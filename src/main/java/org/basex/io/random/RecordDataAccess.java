package org.basex.io.random;

import static org.basex.util.BlockAccessUtil.*;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.util.Num;

/**
 * Storage of variable-length records.
 * TODO: decide how records with size > 4096 will be stored
 * @author BaseX Team 2005-12, BSD License
 * @author Dimitar Popov
 *
 * <br/>
 * <p>A file contains two types of blocks: header and data blocks. The records,
 * are stored in data blocks. Each record is identified by a record id with
 * length 40 bits; the highest 28 bit identify the data block; the lowest 12
 * bits identify the record within the data block.</p>
 *
 * <h1>HEADER BLOCKS</h1>
 * Header blocks are organized in a linked list and contain references to the
 * data blocks and how much space is used in each data block. Each header block
 * has the following structure:
 * <ol>
 * <li>NEXT</li>
 * <p>Pointer to the next header block in the list; size: 5 bytes.</p>
 * <li>BLOCKS</li>
 * <p>Pointers to the data blocks managed by the current header block; number:
 * 629; size: 5 bytes.</p>
 * <li>USED</li>
 * <p>Number of used bytes in each data block managed by the current header
 * block; number: 629; size: 12 bits.</p>
 * </ol>
 *
 * <h1>DATA BLOCKS</h1>
 * <p>Data blocks are divided into two areas: data and meta-data area. The data
 * area is at the beginning of the block. The meta-data area is at the
 * end of the block.</p>
 * <ol>
 * <li>DATA AREA</li>
 * <p>The data area contains the records. A record is a contiguous sequence of
 * bytes starting with the record data length. The data area may be fragemented
 * due to records being deleted. In order to use the space of deleted records,
 * the data area can be de-fragmented.</p>
 * <li>META-DATA AREA</li>
 * <p>The meta data area has the following structure (the fields are stored in
 * reverse order starting from the end of the data block):</p>
 * <ol>
 * <li>SIZE</li>
 * <p>Size shows how many bytes does the data area occupy. More precisely, it
 * shows the last used byte + 1 used by the data area.</p>
 * <li>NUM</li>
 * <p>Number of slots, allocated in this data block.</p>
 * <li>SLOTS</li>
 * <p>Slots are offsets from the beginning of the data area, which show where a
 * record is stored. The index of a slot is used as the lowest 12 bits in a
 * record id. If a record is deleted, then the value stored in a slot is set to
 * NIL. If a record, which occupies the last slot, is deleted, then the number
 * of slots is decremented.</p>
 * </ol>
 * </ol>
 */
public class RecordDataAccess {
  /** Current data block. */
  private final DataBlock block = new DataBlock();
  /** Current header block. */
  private final HeaderBlock header = new HeaderBlock();

  /**
   * Constructor; open a file for data block access.
   * @param f file
   * @throws IOException I/O exception
   */
  public RecordDataAccess(final File f) throws IOException {
    header.da = block.da = new BlockDataAccess(f);
    if(header.da.length() == 0) {
      header.addr = header.da.createBlock();
      header.num = 0;
    }
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    if(block.dirty) block.writeMetaData();
    if(header.dirty) header.write();
    block.da.flush();
    header.da.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    flush();
    block.da.close();
    header.da.close();
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    block.gotoBlock(getBlockAddr(block(rid)));

    block.da.off = block.slots[slot(rid)];
    return block.da.readToken();
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int slot = slot(rid);
    final int blockNum = block(rid);
    final int blockIndex = blockNum % HeaderBlock.BLOCKS;
    block.gotoBlock(getBlockAddr(blockNum));

    // read the record length
    block.da.off = block.slots[slot];
    int len = block.da.readNum();
    len += Num.length(len);
    // decrease size
    block.size -= len;

    if(slot == block.num) {
      // decrease the size of the meta data
      block.metaDataSize -= DataBlock.OFFSET_SIZE;
      // if slot is even, then 1 byte will be freed; else 2 bytes
      header.used[blockIndex] -= (block.num & 1) == 0 ? 1 : 2;
      // the record from the last slot is deleted: decrease number of slots
      --block.num;
    }

    header.used[blockIndex] -= len;
    header.dirty = true;

    if(block.num > 0) {
      block.slots[slot] = DataBlock.NIL;
      block.dirty = true;
    } else {
      block.da.deleteBlock(block.addr);
      block.addr = -1;
      header.blocks[blockIndex] = HeaderBlock.NIL;
    }
  }

  /**
   * Insert a record into the next possible area starting from the current
   * position (i.e. does not traverse all headers).
   * @param record record data
   * @return record id
   */
  public long append(final byte[] record) {
    final int blockNum = findBlock(record.length);
    final int blockIndex = header.last = blockNum % HeaderBlock.BLOCKS;
    block.gotoBlock(header.blocks[blockIndex]);

    // write the record data
    final int len = Num.length(record.length) + record.length;
    final int off = block.da.off = block.allocate(len);
    block.da.writeToken(record);
    // increase size
    block.size += len;

    final int slot = block.findEmptySlot();
    if(slot == block.num) {
      // new slot will be allocated
      ++block.num;
      // increase the size of the meta data
      block.metaDataSize += DataBlock.OFFSET_SIZE;
      // if slot is even, then 1 new byte will be allocated; else 2 bytes
      header.used[blockIndex] += (block.num & 1) == 0 ? 1 : 2;
    }

    header.used[blockIndex] += len;
    header.dirty = true;

    block.slots[slot] = off;
    block.dirty = true;

    return (((long) blockNum) << IO.BLOCKPOWER) | slot;
  }

  /**
   * Insert a record.
   * @param record record data
   * @return record id
   */
  public long insert(final byte[] record) {
    header.gotoHeader(0);
    header.last = 0;
    return append(record);
  }

  /**
   * Find a data block with enough free space; if no existing data block has
   * enough space, then a new data block is allocated; if a data block cannot
   * be allocated in one of the existing header blocks, a new header block will
   * be allocated.
   * @param rsize record size
   * @return data block index which has enough space
   */
  private int findBlock(final int rsize) {
    // space needed is record + record length + slot
    final int size = rsize + Num.length(rsize) + 2;
    // max used space in a block in order to be able to store the record
    final int max = IO.BLOCKSIZE - size;

    // search existing header blocks for a data block with enough space
    long headerAddr = header.addr;
    do {
      header.gotoBlock(headerAddr);
      // check the data blocks of the header for enough space
      for(int i = header.last; i < header.used.length; ++i) {
        if(header.used[i] <= max) {
          if(header.blocks[i] == HeaderBlock.NIL) {
            // the reference is empty: allocate new data block
            header.blocks[i] = block.da.createBlock();
            header.used[i] = 3;
            header.dirty = true;
          }
          return i + header.num * HeaderBlock.BLOCKS;
        }
      }
      ++header.num;
      headerAddr = header.next;
    } while(headerAddr != HeaderBlock.NIL);

    // no header block has empty data blocks: allocate a new header block
    header.next = header.da.createBlock();
    header.dirty = true;

    // allocate the first data block of the new header block
    header.gotoBlock(header.next);
    header.blocks[0] = block.da.createBlock();
    header.used[0] = 3;
    header.dirty = true;

    return header.num * HeaderBlock.BLOCKS;
  }

  /**
   * Get the address of a data block with a given index.
   * @param blockIndex data block index
   * @return block address
   */
  private long getBlockAddr(final int blockIndex) {
    header.gotoHeader(blockIndex / HeaderBlock.BLOCKS);
    return header.blocks[blockIndex % HeaderBlock.BLOCKS];
  }

  /**
   * Extract the block index from a record id.
   * @param rid record id
   * @return block index
   */
  private static int block(final long rid) {
    return (int) (rid >>> IO.BLOCKPOWER);
  }

  /**
   * Extract the slot index from a record id.
   * @param rid record id
   * @return slot index
   */
  private static int slot(final long rid) {
    return (int) (rid & DataBlock.SLOTMASK);
  }
}

/** Common fields for both header and data blocks. */
abstract class Block {
  /** Underlying data file. */
  protected BlockDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;
}

/** Header block. */
class HeaderBlock extends Block {
  /** Size of a block reference in bytes. */
  static final int REFSIZE = 5;
  /** Size of a block reference in bits. */
  static final int REFSIZEBITS = REFSIZE << 3;
  /**
   * Number of data blocks per header; a ref (40 bits) and number of used bytes
   * (12 bits) are stored. The first ref in the block is the next header.
   */
  static final int BLOCKS = ((IO.BLOCKSIZE << 3) - REFSIZEBITS) /
      (REFSIZEBITS + IO.BLOCKPOWER);
  /** Invalid block reference. */
  static final long NIL = 0L;

  /** Block index in the list of headers. */
  int num = -1;
  /** Data block last used for insertion. */
  int last;

  // data stored on disk
  /** Next header block; {@link #NIL} if the last one. */
  long next = NIL;
  /** Id numbers of blocks managed by this header block. */
  final long[] blocks = new long[BLOCKS];
  /** Used space in each block. */
  final int[] used = new int[BLOCKS];

  /**
   * Go to a header block.
   * @param n header block index
   */
  void gotoHeader(final int n) {
    if(num == n) return;
    // start from the beginning, if n is smaller than the current header index
    if(num > n) {
      gotoBlock(0L);
      num = 0;
    }
    // scan headers, until the required index is reached
    // TODO
    //for(; num < n && next != HeaderBlock.NIL; ++num) gotoBlock(next);
    for(; num < n; ++num) gotoBlock(next);
  }

  /**
   * Go to the header block at the specified address.
   * @param blockAddr block address
   */
  void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) return;
    if(dirty) write();
    addr = blockAddr;
    read();
  }

  /** Read the header data from the underlying storage. */
  void read() {
    da.gotoBlock(addr);
    last = 0;

    // NEXT is stored first
    next = da.read5();

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i) blocks[i] = da.read5();

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REFSIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      used[i] = da.readLow12Bits(p, ++p);
      used[i + 1] = da.readHigh12Bits(p, ++p);
    }
    used[n] = da.readLow12Bits(p, ++p);
  }

  /** Write the header data to the underlying storage. */
  void write() {
    da.gotoBlock(addr);

    // NEXT is stored first
    da.write5(next);

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i) da.write5(blocks[i]);

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REFSIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      da.writeLow12Bits(p, ++p, used[i]);
      da.writeHigh12Bits(p, ++p, used[i + 1]);
    }
    da.writeLow12Bits(p, ++p, used[n]);
    dirty = false;
  }
}

/** Data block. */
class DataBlock extends Block {
  /** Bit mask used to extract the slot number from a record id. */
  static final long SLOTMASK = (1L << IO.BLOCKPOWER) - 1L;
  /** Size of an offset within a block in bits. */
  static final int OFFSET_SIZE = IO.BLOCKPOWER;
  /** Maximal number of records in a data block. */
  private static final int MAX_RECORDS = IO.BLOCKSIZE >>> 1;
  /** Empty slot marker. */
  static final int NIL = (int) SLOTMASK;

  /** Size of the meta data of the current block. */
  int metaDataSize;

  // fields stored in the block:
  /** Data area size (in bytes). */
  int size;
  /** Number of slots. */
  int num;
  /** Slots with record offsets in the data area. */
  final int[] slots = new int[MAX_RECORDS];

  /**
   * Go to the data block at the specified address.
   * @param blockAddr block address
   */
  void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) {
      if(da.blockPos() != blockAddr) da.gotoBlock(blockAddr);
      return;
    }

    if(dirty) writeMetaData();
    addr = blockAddr;
    readMetaData();
  }

  /** Read the meta-data of a data block. */
  void readMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    size = da.readLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2);
    // NUMBER OF SLOTS is stored the next 12 bits
    num = da.readHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      slots[i] = da.readLow12Bits(p, --p);
      slots[i + 1] = da.readHigh12Bits(p, --p);
    }
    if(odd) slots[n] = da.readLow12Bits(p, --p);

    metaDataSize = (num + 2) * DataBlock.OFFSET_SIZE;
  }

  /** Write the meta-data of a data block. */
  void writeMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    da.writeLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2, size);
    // NUMBER OF SLOTS is stored the next 12 bits
    da.writeHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3, num);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      da.writeLow12Bits(p, --p, slots[i]);
      da.writeHigh12Bits(p, --p, slots[i + 1]);
    }
    if(odd) da.writeLow12Bits(p, --p, slots[n]);
    dirty = false;
  }

  /**
   * Find an empty slot; if none is available, create a new one at the end.
   * @return index of the empty slot
   */
  int findEmptySlot() {
    for(int r = 0; r < num; ++r) if(slots[r] == DataBlock.NIL) return r;
    return num;
  }

  /**
   * Allocate a given number of bytes from the free space area.
   * @param l number of bytes
   * @return offset within the block where the bytes have been allocated
   */
  int allocate(final int l) {
    // re-organize records, if not enough space
    if(l > IO.BLOCKSIZE - size - (int) divRoundUp(metaDataSize, 8)) compact();
    return size;
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
    // adjust the size value
    size = pos;
  }
}
