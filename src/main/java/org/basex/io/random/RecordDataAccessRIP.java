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
}
public class RecordDataAccessRIP extends Blocks {
  /** Header block file. */
  private final BlockDataAccess headerFile;

  private final DataBlock block = new DataBlock();

  private final HeaderBlock header = new HeaderBlock();

  /**
   * Constructor; open a file for data block access.
   * @param f file
   * @throws IOException I/O exception
   */
  public RecordDataAccessRIP(final File f) throws IOException {
    super(f);
    headerFile = da;
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    headers.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    headers.close();
  }

  @Override
  protected void gotoBlock(final long blockAddr) {
    if(block.addr == blockAddr) {
      if(da.blockPos() != blockAddr) da.gotoBlock(blockAddr);
      return;
    }
    super.gotoBlock(blockAddr);
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    gotoBlock(getBlockAddr(block(rid)));

    da.off = block.slots[slot(rid)];
    return da.readToken();
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int slot = slot(rid);
    final int blockNum = block(rid);
    final int blockIndex = blockNum % HeaderBlocks.BLOCKS;
    gotoBlock(getBlockAddr(blockNum));

    // read the record length
    da.off = block.slots[slot];
    int len = da.readNum();
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

    block.slots[slot] = DataBlock.NIL;
    block.dirty = true;
  }

  /**
   * Insert a record.
   * @param record record data
   * @return record id
   */
  public long insert(final byte[] record) {
    final int blockNum = findBlock(record.length);
    final int blockIndex = blockNum % HeaderBlocks.BLOCKS;
    gotoBlock(header.blocks[blockIndex]);

    // write the record data
    final int len = Num.length(record.length) + record.length;
    final int off = da.off = allocate(len);
    da.writeToken(record);
    // increase size
    block.size += len;

    final int slot = findEmptySlot();
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

  @Override
  protected void readMetaData() {
    da.gotoBlock(block.addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    block.size = da.readLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2);
    // NUMBER OF SLOTS is stored the next 12 bits
    block.num = da.readHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (block.num & 1) == 1;
    final int n = odd ? block.num - 1 : block.num;
    for(int i = 0; i < n; i += 2, --p) {
      block.slots[i] = da.readLow12Bits(p, --p);
      block.slots[i + 1] = da.readHigh12Bits(p, --p);
    }
    if(odd) block.slots[n] = da.readLow12Bits(p, --p);

    block.metaDataSize = (block.num + 2) * DataBlock.OFFSET_SIZE;
  }

  @Override
  protected void writeMetaData() {
    da.gotoBlock(block.addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    da.writeLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2, block.size);
    // NUMBER OF SLOTS is stored the next 12 bits
    da.writeHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3, block.num);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (block.num & 1) == 1;
    final int n = odd ? block.num - 1 : block.num;
    for(int i = 0; i < n; i += 2, --p) {
      da.writeLow12Bits(p, --p, block.slots[i]);
      da.writeHigh12Bits(p, --p, block.slots[i + 1]);
    }
    if(odd) da.writeLow12Bits(p, --p, block.slots[n]);
  }

  /**
   * Find an empty slot; if none is available, create a new one at the end.
   * @return index of the empty slot
   */
  private int findEmptySlot() {
    for(int r = 0; r < block.num; ++r)
      if(block.slots[r] == DataBlock.NIL) return r;
    return block.num;
  }

  /**
   * Allocate a given number of bytes from the free space area.
   * @param l number of bytes
   * @return offset within the block where the bytes have been allocated
   */
  private int allocate(final int l) {
    // re-organize records, if not enough space
    if(l > IO.BLOCKSIZE - block.size - (int) divRoundUp(block.metaDataSize, 8))
      compact();
    return block.size;
  }

  /** Compact records in a contiguous area. */
  private void compact() {
    int pos = 0;
    for(int i = 0; i < block.num; ++i) {
      // read the length of the record (size + data)
      da.off = block.slots[i];
      int len = da.readNum();
      len += Num.length(len);

      if(block.slots[i] > pos) {
        // there is unused space: shift the record (size + data) forwards
        da.off = block.slots[i];
        final byte[] record = da.readBytes(len);
        da.off = pos;
        da.writeBytes(record);
        block.slots[i] = pos;
      } else if(block.slots[i] < pos) {
        throw new RuntimeException("Not expected");
      }
      // next position should be right after the current record
      pos += len;
    }
    // adjust the size value
    block.size = pos;
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
    long n = 0L;
    header.num = 0;
    do {
      gotoBlock(n);
      // check the data blocks of the header for enough space
      for(int i = 0; i < header.used.length; ++i) {
        if(header.used[i] <= max) {
          if(header.blocks[i] == HeaderBlock.NIL) {
            // the reference is empty: allocate new data block
            header.blocks[i] = da.createBlock();
            header.used[i] = 3;
            header.dirty = true;
          }
          return i + header.num * HeaderBlock.BLOCKS;
        }
      }
      ++header.num;
      n = header.next;
    } while(n != HeaderBlock.NIL);

    // no header block has empty data blocks: allocate new a new header block
    header.next = headerFile.createBlock();
    header.dirty = true;

    // allocate the first data block of the new header block
    gotoBlock(header.next);
    header.blocks[0] = da.createBlock();
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
    gotoHeaderBlock(blockIndex / HeaderBlock.BLOCKS);
    return header.blocks[blockIndex % HeaderBlock.BLOCKS];
  }

  private void readHeaderMetaData() {
    headerFile.gotoBlock(header.addr);

    // NEXT is stored first
    header.next = headerFile.read5();

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i)
      header.blocks[i] = headerFile.read5();

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REFSIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      header.used[i] = headerFile.readLow12Bits(p, ++p);
      header.used[i + 1] = headerFile.readHigh12Bits(p, ++p);
    }
    header.used[n] = headerFile.readLow12Bits(p, ++p);
  }

  private void writeHeaderMetaData() {
    headerFile.gotoBlock(header.addr);

    // NEXT is stored first
    headerFile.write5(header.next);

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i)
      headerFile.write5(header.blocks[i]);

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REFSIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      headerFile.writeLow12Bits(p, ++p, header.used[i]);
      headerFile.writeHigh12Bits(p, ++p, header.used[i + 1]);
    }
    headerFile.writeLow12Bits(p, ++p, header.used[n]);
  }

  /**
   * Go to a header block.
   * @param n header block index
   */
  private void gotoHeaderBlock(final int n) {
    if(header.num == n) return;
    // go to the first header, if n is smaller than the current header index
    if(header.num > n) {
      gotoBlock(0L);
      header.num = 0;
    }
    // scan headers, until the required index is reached
    for(; header.num < n; ++header.num) gotoBlock(header.next);
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

/** Common class for data and header blocks. */
abstract class Blocks {
  /** Size of a block in bits. */
  static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;

  /** Underlying data file. */
  protected final BlockDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;

  /**
   * Constructor.
   * @param data data
   */
  public Blocks(final BlockDataAccess data) {
    da = data;
  }

  /**
   * Constructor; open a file for block access.
   * @param file file
   * @throws IOException I/O exception
   */
  public Blocks(final File file) throws IOException {
    da = new BlockDataAccess(file);
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    writeMetaData();
    dirty = false;
    da.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    if(dirty) flush();
    da.close();
  }

  /**
   * Go to the data block with the specified block id number.
   * @param blockAddr block address
   */
  protected void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) return;
    if(dirty) {
      writeMetaData();
      dirty = false;
    }
    addr = blockAddr;
    readMetaData();
  }

  /** Read meta data from the current block. */
  protected abstract void readMetaData();

  /** Write meta data to the underlying storage.  */
  protected abstract void writeMetaData();
}

abstract class Block {
  /** Size of a block in bits. */
  static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;

  /** Underlying data file. */
  protected BlockDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;
}

class HeaderBlock extends Block {
  /** Size of a block reference in bytes. */
  static final int REFSIZE = 5;
  /** Size of a block reference in bits. */
  static final int REFSIZEBITS = REFSIZE * Byte.SIZE;
  /** Number of data blocks per header. */
  static final int BLOCKS = (Blocks.BLOCKSIZEBITS - REFSIZEBITS) /
      (REFSIZEBITS + IO.BLOCKPOWER);
  /** Invalid block reference. */
  static final long NIL = 0L;

  /** Block index in the list of headers. */
  int num = -1;

  // data stored on disk
  /** Next header block; {@link #NIL} if the last one. */
  long next = NIL;
  /** Id numbers of blocks managed by this header block. */
  final long[] blocks = new long[BLOCKS];
  /** Used space in each block. */
  final int[] used = new int[BLOCKS];
}
