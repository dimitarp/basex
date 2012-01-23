package org.basex.io.random;

import static org.basex.io.random.Blocks.*;
import static org.basex.util.Reflect.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.basex.util.Reflect;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class RecordAccessTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public final void testFlush() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testClose() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testReadMetaData() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testWriteMetaData() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testRecordAccess() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testSelect() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testDelete() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testInsert() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testBlock() throws Exception {
    assertEquals(0x7FFFFFFF,
        invoke(
            method(RecordAccess.class, "block", long.class),
            0x0000007FFFFFFF000L));
  }

  @Test
  public final void testSlot() throws Exception {
    assertEquals(0xFAC,
        invoke(
            method(RecordAccess.class, "slot", long.class),
            0x000000FFFFFFFFACL));
  }

  @Test
  public final void testHeaderIndex() throws Exception {
    final int expected = 23;
    assertEquals(expected,
        invoke(
            method(HeaderBlocks.class, "headerIndex", int.class),
            348 + expected * field(HeaderBlocks.class, "BLOCKS").getInt(null)));
  }


  static Object invoke(final Method m, final Object... args)
      throws Exception {
    return Reflect.invoke(m, (Object) null, args);
  }

  static Field field(final Class<?> cls, final String name) {
    try {
      final Field f = cls.getDeclaredField(name);
      if(!f.isAccessible()) f.setAccessible(true);
      return f;
    } catch(Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class HeaderBlocksTest {

    private final Method gotoHeaderBlock = method(
        HeaderBlocks.class, "gotoHeaderBlock", int.class);

    private final Field num = field(HeaderBlocks.class, "num");

    private final Field next = field(HeaderBlocks.class, "next");

    private static int BLOCKS;

    private long[] blocks;

    private int[] free;

    Buffer buffer;

    @InjectMocks
    BlockDataAccess da;

    HeaderBlocks headers;

    @Mock
    RandomAccessFile file;

    @BeforeClass
    public static void init() throws Exception {
      BLOCKS = field(HeaderBlocks.class, "BLOCKS").getInt(null);
    }

    @Before
    public void setUp() throws Exception {
      buffer = new Buffer();
      blocks = new long[BLOCKS];
      free = new int[BLOCKS];
      field(HeaderBlocks.class, "blocks").set(headers, blocks);
      field(HeaderBlocks.class, "free").set(headers, free);
      field(Blocks.class, "da").set(headers, da);
    }

    @Test
    public void testReadMetaData() throws Exception {
      final long addr = 10L;
      headers.addr = addr;

      final long nextAddr = 0xF000000DFFL;
      buffer.data[0] = (byte) 0xFF;
      buffer.data[1] = (byte) 0x0D;
      buffer.data[4] = (byte) 0xF0;

      when(da.buffer(anyBoolean())).thenReturn(buffer);

      doCallRealMethod().when(headers).readMetaData();

      headers.readMetaData();

      assertEquals(nextAddr, next.getLong(headers));
    }

    @Test
    public void testGetBlockAddr() throws Exception {
      blocks[0] = 10L;
      blocks[1] = 11L;
      blocks[2] = 12L;

      doCallRealMethod().when(headers).getBlockAddr(anyInt());

      assertEquals(10L, headers.getBlockAddr(0));
      assertEquals(11L, headers.getBlockAddr(1));
      assertEquals(12L, headers.getBlockAddr(BLOCKS + 2));
    }

    @Test
    public void testGotoHeaderBlock() throws Exception {
      final int h = 2;
      Reflect.invoke(gotoHeaderBlock, headers, h);
      verify(headers, times(h)).gotoBlock(anyLong());
    }

    @Test
    public void testGotoHeaderBlockSame() throws Exception {
      final int h = 3;
      num.set(headers, h);
      Reflect.invoke(gotoHeaderBlock, headers, h);
      verify(headers, times(0)).gotoBlock(anyLong());
    }

    @Test
    public void testGotoHeaderBlockSmaller() throws Exception {
      final int h = 3;
      num.set(headers, h + 2);
      Reflect.invoke(gotoHeaderBlock, headers, h);
      verify(headers, times(h + 1)).gotoBlock(anyLong());
    }

  }

  @RunWith(MockitoJUnitRunner.class)
  public static class BlocksTest {

    @Mock
    BlockDataAccess da;

    @Mock
    Blocks blocks;

    @Before
    public void setUp() throws Exception {
      field(Blocks.class, "da").set(blocks, da);
      doCallRealMethod().when(blocks).flush();
      doCallRealMethod().when(blocks).close();
      doCallRealMethod().when(blocks).gotoBlock(anyLong());
    }

    @Test
    public final void testFlush() throws Exception {
      blocks.flush();

      assertFalse(blocks.dirty);

      verify(blocks).writeMetaData();
      verify(da).flush();
    }

    @Test
    public final void testClose() throws Exception {
      blocks.close();

      assertFalse(blocks.dirty);

      verify(blocks, never()).writeMetaData();
      verify(blocks, never()).flush();
      verify(da, never()).flush();
    }

    @Test
    public final void testCloseDirty() throws Exception {
      blocks.dirty = true;

      blocks.close();

      assertFalse(blocks.dirty);

      verify(blocks).writeMetaData();
      verify(blocks).flush();
      verify(da).flush();
    }

    @Test
    public final void testGotoBlock() {
      final long blockAddress = 10L;

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertFalse(blocks.dirty);

      verify(blocks).readMetaData();
      verify(blocks, never()).writeMetaData();
    }

    @Test
    public final void testGotoBlockSame() {
      final long blockAddress = 10L;

      blocks.dirty = true;
      blocks.addr = blockAddress;

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertTrue(blocks.dirty);

      verify(blocks, never()).readMetaData();
      verify(blocks, never()).writeMetaData();
    }

    @Test
    public final void testGotoBlockDirty() {
      final long blockAddress = 10L;

      final InOrder inOrder = inOrder(blocks);
      blocks.dirty = true;

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertFalse(blocks.dirty);

      inOrder.verify(blocks).writeMetaData();
      inOrder.verify(blocks).readMetaData();
    }

    @Test
    public final void testReadFirst12Bits() {
      assertEquals(0xFFF, readFirst12Bits((byte) 0xFF, (byte) 0x0F));
    }

    @Test
    public final void testReadLast12Bits() {
      assertEquals(0xFFF, readLast12Bits((byte) 0xF0, (byte) 0xFF));
    }

    @Test
    public final void testWriteFirst12Bits() {
      final byte[] data = new byte[2];
      writeFirst12Bits(data, 0, 1, 0xFFF);
      assertArrayEquals(new byte[] { (byte) 0xFF, (byte) 0x0F }, data);
    }

    @Test
    public final void testWriteLast12Bits() {
      final byte[] data = new byte[2];
      writeLast12Bits(data, 0, 1, 0xFFF);
      assertArrayEquals(new byte[] { (byte) 0xF0, (byte) 0xFF }, data);
    }
  }
}
