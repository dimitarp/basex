package org.basex.io.random;

import static org.junit.Assert.*;

import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.junit.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

@Ignore
public class BlockFileAccessPerfTest {
  private static final IOFile FILE = IOFile.get("block-file-access");

  //private static final long LEN = 1L << 32;
  private static final long LEN = 1L << 30;
  //private static final long LEN = 1L << 27;

  private static final int RANDOM_OPS = 5000;

  private BlockFileAccess file;

  private Buffer buffer;

  //@BeforeClass
  public static void setUpBeforeClass() throws IOException {
    if (FILE.exists()) cleanUpAfterClass();

    final FileChannel f = new RandomAccessFile(FILE.file(), "rw").getChannel();

    final int bufferSize = 1 << 27;
    final ByteBuffer b = ByteBuffer.allocateDirect(bufferSize);

    final int iterations = (int) (LEN / bufferSize);
    final int longsInBuffer = bufferSize / Long.SIZE;
    final Random r = new Random(System.currentTimeMillis());
    try {
      for (int i = 0; i < iterations; i++) {
        b.rewind();
        for (int p = 0; p < longsInBuffer; p++) b.putLong(r.nextLong());
        b.rewind();
        f.write(b);
      }
    } finally {
      f.close();
    }
  }

  //@AfterClass
  public static void cleanUpAfterClass() {
    assertTrue(FILE.delete());
  }

  @Before
  public void setUpBeforeTest_MMap() throws IOException {
    setUpBeforeClass();
    RandomAccessFile f = new RandomAccessFile(FILE.file(), "rw");
    final Performance perf = new Performance();

    openMemoryMappedBlockFileAccess(f);
    //openFileChannelBlockFileAccess(f);
    //openRandomAccessFileBlockFileAccess(f);

    Util.outln("File open: %", perf.getTime());
  }

  @After
  public void cleanUpAfterTest() throws IOException {
    final Performance perf = new Performance();
    file.close();
    Util.outln("File close: %", perf.getTime());
    cleanUpAfterClass();
  }

  private void openMemoryMappedBlockFileAccess(RandomAccessFile f) throws IOException {
    Util.outln("MMap implementation");
    file = new MemoryMappedBlockFileAccess(f.getChannel());
    buffer = new BlockBuffer(IO.BLOCKSIZE);
  }

  private void openFileChannelBlockFileAccess(RandomAccessFile f) throws IOException {
    Util.outln("NIO implementation");
    file = new FileChannelBlockFileAccess(f.getChannel());
    buffer = new BlockBuffer(IO.BLOCKSIZE);
  }

  private void openRandomAccessFileBlockFileAccess(RandomAccessFile f) throws IOException {
    Util.outln("Classic implementation");
    file = new RandomAccessFileBlockFileAccess(f);
    buffer = new ByteArrayBuffer(IO.BLOCKSIZE);
  }


  @Test
  public void testSetup() throws IOException {
    RandomAccessFile f = new RandomAccessFile(FILE.file(), "r");
    assertEquals(LEN, f.length());
  }

  @Test
  public void testSequentialReads() throws IOException {
    final Performance perf = new Performance();

    for (long p = 0L; p < LEN; p += IO.BLOCKSIZE) {
      buffer.setPos(p);
      file.read(buffer);
    }

    Util.outln("Sequential reads: %", perf.getTime());
  }

  @Test
  public void testRandomReads() throws IOException {
    final Performance perf = new Performance();

    final int blocks = (int) (LEN / IO.BLOCKSIZE);
    for (long p = 0L; p < RANDOM_OPS; ++p) {
      buffer.setPos(((long) (random.nextInt(blocks) & Integer.MAX_VALUE)) * IO.BLOCKSIZE);
      file.read(buffer);
    }

    Util.outln("Random reads: %", perf.getTime());
  }

  @Test
  public void testSequentialWrites() throws IOException {
    final Performance perf = new Performance();

    for (long p = 0L; p < LEN; p += IO.BLOCKSIZE) {
      fillBufferWithRandomBytes(buffer);
      buffer.setPos(p);
      file.write(buffer);
    }

    Util.outln("Sequential writes: %", perf.getTime());
  }

  @Test
  public void testRandomWrites() throws IOException {
    final Performance perf = new Performance();

    final int blocks = (int) (LEN / IO.BLOCKSIZE);
    for (long p = 0L; p < RANDOM_OPS; ++p) {
      fillBufferWithRandomBytes(buffer);
      buffer.setPos(((long) (random.nextInt(blocks) & Integer.MAX_VALUE)) * IO.BLOCKSIZE);
      file.write(buffer);
    }

    Util.outln("Random writes: %", perf.getTime());
  }

  private final Random random = new Random(System.currentTimeMillis());
  private final byte[] tmp = new byte[IO.BLOCKSIZE];

  private void fillBufferWithRandomBytes(Buffer b) {
    random.nextBytes(tmp);
    b.copyFrom(0, tmp, 0, tmp.length);
  }
}
