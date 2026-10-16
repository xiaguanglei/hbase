/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.ByteArrayCacheable;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link BlockCacheUtil#validateBlockAddition} compares two {@link HFileBlock}s in
 * place, with the same outcome as comparing their serialized forms, whatever their buffers look
 * like.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestCompareCacheBlock {

  private static final int HEADER_SIZE = HConstants.HFILEBLOCK_HEADER_SIZE;

  private static final int BLOCK_LENGTH = HEADER_SIZE + 5000;

  private static final BlockCacheKey KEY = new BlockCacheKey("test", 0);

  /** Buffer layouts of a cached block, as described for blocks of BLOCK_LENGTH bytes. */
  private enum Layout {
    /** One heap ByteBuffer, as allocated on heap. */
    HEAP,
    /** A heap ByteBuffer sliced out of a larger one. */
    HEAP_SLICE,
    /** A direct ByteBuffer sliced out of a larger one. */
    DIRECT,
    /** Pooled direct ByteBuffers, the last one partly used. */
    POOLED,
    /** Pooled direct ByteBuffers, then a heap ByteBuffer holding the rest. */
    POOLED_HEAP_TAIL,
    /** Pooled ByteBuffers holding more than the block, limited to it. */
    TRIMMED,
    /** ByteBuffers of uneven sizes, a single byte and empty ones among them. */
    UNEVEN
  }

  @Test
  public void testIdenticalBlocksCompareEqual() {
    Random rnd = new Random(1);
    for (int length : new int[] { 0, 1, HEADER_SIZE, BLOCK_LENGTH }) {
      byte[] content = randomBytes(rnd, length);
      for (Layout leftLayout : Layout.values()) {
        for (Layout rightLayout : Layout.values()) {
          String msg = leftLayout + " vs " + rightLayout + ", length " + length;
          ByteBuff leftBuf = newBuffer(leftLayout, content, rnd);
          ByteBuff rightBuf = newBuffer(rightLayout, content, rnd);
          HFileBlock left = newBlock(leftBuf, true, 4096, -1);
          HFileBlock right = newBlock(rightBuf, true, 4096, -1);
          assertEquals(0, compareInPlace(left, leftBuf, right, rightBuf, msg), msg);
        }
      }
    }
  }

  @Test
  public void testOneDifferingByte() {
    Random rnd = new Random(2);
    byte[] content = randomBytes(rnd, BLOCK_LENGTH);
    for (Layout leftLayout : Layout.values()) {
      for (Layout rightLayout : Layout.values()) {
        String pair = leftLayout + " vs " + rightLayout;
        ByteBuff leftBuf = newBuffer(leftLayout, content, rnd);
        ByteBuff rightBuf = newBuffer(rightLayout, content, rnd);
        HFileBlock left = newBlock(leftBuf, true, 4096, -1);
        HFileBlock right = newBlock(rightBuf, true, 4096, -1);
        Set<Integer> indexes = new TreeSet<>();
        // A header byte, the first and the last data byte, and the bytes around item boundaries.
        indexes.addAll(Arrays.asList(HEADER_SIZE / 2, HEADER_SIZE, content.length - 1));
        addItemBoundaries(indexes, leftBuf);
        addItemBoundaries(indexes, rightBuf);
        for (int index : indexes) {
          String msg = pair + ", byte " + index;
          byte b = rightBuf.get(index);
          for (int delta : new int[] { 1, -1 }) {
            rightBuf.put(index, (byte) (b + delta));
            assertNotEquals(0, compareInPlace(left, leftBuf, right, rightBuf, msg), msg);
          }
          rightBuf.put(index, b);
        }
        assertEquals(0, compareInPlace(left, leftBuf, right, rightBuf, pair), pair);
      }
    }
  }

  @Test
  public void testDifferentOffsetOrChecksumFlag() {
    Random rnd = new Random(3);
    byte[] content = randomBytes(rnd, BLOCK_LENGTH);
    for (Layout layout : Layout.values()) {
      ByteBuff leftBuf = newBuffer(Layout.HEAP, content, rnd);
      ByteBuff rightBuf = newBuffer(layout, content, rnd);
      HFileBlock left = newBlock(leftBuf, true, 4096, -1);
      // The unset offset, -1, is serialized as all ones; Long.MIN_VALUE starts with a one.
      for (long offset : new long[] { 4095, 4097, 0, -1, Long.MIN_VALUE }) {
        String msg = layout + ", offset " + offset;
        HFileBlock right = newBlock(rightBuf, true, offset, -1);
        assertNotEquals(0, compareInPlace(left, leftBuf, right, rightBuf, msg), msg);
      }
      String msg = layout + ", no checksums";
      HFileBlock right = newBlock(rightBuf, false, 4096, -1);
      assertNotEquals(0, compareInPlace(left, leftBuf, right, rightBuf, msg), msg);
    }
  }

  @Test
  public void testNextBlockOnDiskSizeIsNotCompared() {
    Random rnd = new Random(4);
    byte[] content = randomBytes(rnd, BLOCK_LENGTH);
    ByteBuff withoutNextBuf = newBuffer(Layout.POOLED, content, rnd);
    ByteBuff withNextBuf = newBuffer(Layout.DIRECT, content, rnd);
    HFileBlock withoutNext = newBlock(withoutNextBuf, true, 4096, -1);
    HFileBlock withNext = newBlock(withNextBuf, true, 4096, 5100);
    assertEquals(0, compareInPlace(withoutNext, withoutNextBuf, withNext, withNextBuf, "next"));
    // As before, the difference decides whether the new block replaces the cached one.
    assertEquals(-1 - 5100, BlockCacheUtil.validateBlockAddition(withoutNext, withNext, KEY));
    assertEquals(5100 + 1, BlockCacheUtil.validateBlockAddition(withNext, withoutNext, KEY));
  }

  @Test
  public void testSameOutcomeAsComparingSerializedBlocks() {
    long seed = ThreadLocalRandom.current().nextLong();
    Random rnd = new Random(seed);
    for (int i = 0; i < 1000; i++) {
      String msg = "seed " + seed + ", iteration " + i;
      int length = HEADER_SIZE + rnd.nextInt(4096);
      if (rnd.nextInt(10) == 0) {
        // Sometimes shorter than a header, down to empty.
        length = rnd.nextInt(HEADER_SIZE + 1);
      }
      byte[] content = randomBytes(rnd, length);
      byte[] otherContent = content;
      boolean checksums = rnd.nextBoolean();
      boolean otherChecksums = checksums;
      long offset = randomOffset(rnd);
      long otherOffset = offset;
      int next = randomNextBlockOnDiskSize(rnd);
      int otherNext = next;
      BlockType otherType = BlockType.DATA;
      long otherPrev = -1;
      switch (rnd.nextInt(7)) {
        case 0:
          // The same block twice.
          break;
        case 1:
          otherContent = withOneByteChanged(content, rnd);
          break;
        case 2:
          otherOffset = randomOffset(rnd);
          break;
        case 3:
          otherChecksums = !checksums;
          break;
        case 4:
          // Not serialized, as newBlock does not write them into the header.
          otherNext = randomNextBlockOnDiskSize(rnd);
          otherType = BlockType.values()[rnd.nextInt(BlockType.values().length)];
          otherPrev = rnd.nextLong();
          break;
        case 5:
          otherContent = withRandomLength(content, rnd);
          break;
        default:
          // The first differing byte decides, wherever else the blocks differ.
          otherContent = withOneByteChanged(content, rnd);
          otherOffset = randomOffset(rnd);
          otherChecksums = rnd.nextBoolean();
          break;
      }
      ByteBuff leftBuf = randomLayout(content, rnd);
      ByteBuff rightBuf = randomLayout(otherContent, rnd);
      HFileBlock left = newBlock(leftBuf, BlockType.DATA, -1, checksums, offset, next);
      HFileBlock right =
        newBlock(rightBuf, otherType, otherPrev, otherChecksums, otherOffset, otherNext);
      leftBuf.position(rnd.nextInt(leftBuf.limit() + 1));
      rightBuf.position(rnd.nextInt(rightBuf.limit() + 1));
      compareInPlace(left, leftBuf, right, rightBuf, msg);
    }
  }

  @Test
  public void testComparingBlocksDoesNotCopyThem() {
    assumeTrue(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean,
      "No com.sun.management.ThreadMXBean");
    ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeTrue(threads.isThreadAllocatedMemorySupported(), "Allocations cannot be measured");
    assumeTrue(threads.getCurrentThreadAllocatedBytes() >= 0, "Allocations are not measured");
    Random rnd = new Random(5);
    // Pooled direct buffers and a heap tail against a single heap buffer.
    byte[] content = randomBytes(rnd, 4 * 1024 * 1024 + HEADER_SIZE);
    HFileBlock cached = newBlock(pooled(content, 64 * 1024, rnd), true, 4096, -1);
    HFileBlock offered = newBlock(ByteBuff.wrap(ByteBuffer.wrap(content)), true, 4096, -1);
    // Load and initialize the classes involved before measuring.
    assertEquals(0, BlockCacheUtil.validateBlockAddition(cached, offered, KEY));
    long before = threads.getCurrentThreadAllocatedBytes();
    int result = BlockCacheUtil.validateBlockAddition(cached, offered, KEY);
    long allocated = threads.getCurrentThreadAllocatedBytes() - before;
    assertEquals(0, result);
    // Comparing serialized copies of the blocks allocated twice the block size here.
    assertTrue(allocated < 64 * 1024,
      "Comparing blocks of " + content.length + " bytes allocated " + allocated + " bytes");
  }

  @Test
  public void testOtherCacheablesStillCompareSerialized() {
    byte[] bytes = Bytes.toBytes("not an HFileBlock");
    Cacheable cached = new ByteArrayCacheable(bytes);
    Cacheable same = new ByteArrayCacheable(bytes.clone());
    assertEquals(0, BlockCacheUtil.validateBlockAddition(cached, same, KEY));
    byte[] otherBytes = bytes.clone();
    otherBytes[otherBytes.length - 1]++;
    assertContentsDiffer(cached, new ByteArrayCacheable(otherBytes), "different bytes");
    // Also when only one of them is an HFileBlock.
    HFileBlock block = newBlock(ByteBuff.wrap(ByteBuffer.wrap(bytes.clone())), true, 0, -1);
    assertContentsDiffer(block, cached, "HFileBlock cached");
    assertContentsDiffer(cached, block, "HFileBlock offered");
  }

  /**
   * Compares left to right in place and checks the result against comparing their serialized forms:
   * both are 0 or neither is, and for buffers of the same length their signs agree, while otherwise
   * the shorter block orders first. Also checks that the result is antisymmetric, that positions,
   * limits and reference counts stay as they were, and that validateBlockAddition agrees.
   */
  private static int compareInPlace(HFileBlock left, ByteBuff leftBuf, HFileBlock right,
    ByteBuff rightBuf, String msg) {
    List<Integer> leftState = positionsAndLimits(leftBuf);
    List<Integer> rightState = positionsAndLimits(rightBuf);
    int leftRefCnt = left.refCnt();
    int rightRefCnt = right.refCnt();
    int result = left.compareSerializedContent(right);
    int reversed = right.compareSerializedContent(left);
    assertEquals(leftState, positionsAndLimits(leftBuf), msg);
    assertEquals(rightState, positionsAndLimits(rightBuf), msg);
    assertEquals(leftRefCnt, left.refCnt(), msg);
    assertEquals(rightRefCnt, right.refCnt(), msg);
    assertEquals(-Integer.signum(result), Integer.signum(reversed), msg);

    int serialized = compareSerialized(left, right);
    assertEquals(serialized == 0, result == 0, msg);
    int leftLength = leftBuf.limit();
    int rightLength = rightBuf.limit();
    if (leftLength == rightLength) {
      assertEquals(Integer.signum(serialized), Integer.signum(result), msg);
    } else {
      // Serialized forms compare like strings, while blocks of different lengths order by length.
      assertEquals(Integer.signum(leftLength - rightLength), Integer.signum(result), msg);
    }
    if (result == 0) {
      assertEquals(left.getNextBlockOnDiskSize() - right.getNextBlockOnDiskSize(),
        BlockCacheUtil.validateBlockAddition(left, right, KEY), msg);
    } else {
      assertContentsDiffer(left, right, msg);
    }
    return result;
  }

  /** How BlockCacheUtil compared blocks before: by serialized copies of both. */
  private static int compareSerialized(Cacheable left, Cacheable right) {
    ByteBuffer l = ByteBuffer.allocate(left.getSerializedLength());
    left.serialize(l, false);
    ByteBuffer r = ByteBuffer.allocate(right.getSerializedLength());
    right.serialize(r, false);
    return Bytes.compareTo(l.array(), l.arrayOffset(), l.limit(), r.array(), r.arrayOffset(),
      r.limit());
  }

  private static void assertContentsDiffer(Cacheable cached, Cacheable offered, String msg) {
    RuntimeException e = assertThrows(RuntimeException.class,
      () -> BlockCacheUtil.validateBlockAddition(cached, offered, KEY), msg);
    assertTrue(e.getMessage().startsWith("Cached block contents differ"), msg);
  }

  private static List<Integer> positionsAndLimits(ByteBuff buf) {
    List<Integer> state = new ArrayList<>();
    state.add(buf.position());
    state.add(buf.limit());
    for (ByteBuffer item : buf.nioByteBuffers()) {
      state.add(item.position());
      state.add(item.limit());
    }
    return state;
  }

  /** Adds the indexes on both sides of each boundary between the items of buf. */
  private static void addItemBoundaries(Set<Integer> indexes, ByteBuff buf) {
    int boundary = 0;
    for (ByteBuffer item : buf.nioByteBuffers()) {
      boundary += item.limit();
      if (boundary > 0 && boundary < buf.limit()) {
        indexes.add(boundary - 1);
        indexes.add(boundary);
      }
    }
  }

  private static HFileBlock newBlock(ByteBuff buf, boolean usesHBaseChecksum, long offset,
    int nextBlockOnDiskSize) {
    return newBlock(buf, BlockType.DATA, -1, usesHBaseChecksum, offset, nextBlockOnDiskSize);
  }

  /**
   * Returns a block over buf. Its header is not filled in, so all serialized block bytes come from
   * buf. Moves the position of buf away from 0 afterwards, which comparing has to ignore.
   */
  private static HFileBlock newBlock(ByteBuff buf, BlockType type, long prevBlockOffset,
    boolean usesHBaseChecksum, long offset, int nextBlockOnDiskSize) {
    int sizeWithoutHeader = Math.max(0, buf.limit() - HEADER_SIZE);
    HFileContext context = new HFileContextBuilder().withHBaseCheckSum(usesHBaseChecksum).build();
    HFileBlock block = new HFileBlock(type, sizeWithoutHeader, sizeWithoutHeader, prevBlockOffset,
      buf, HFileBlock.DONT_FILL_HEADER, offset, nextBlockOnDiskSize, buf.limit(), context,
      ByteBuffAllocator.HEAP);
    buf.position(buf.limit() / 2);
    return block;
  }

  /** Returns a copy of content in the given layout. */
  private static ByteBuff newBuffer(Layout layout, byte[] content, Random rnd) {
    switch (layout) {
      case HEAP:
        return ByteBuff.wrap(ByteBuffer.wrap(content.clone()));
      case HEAP_SLICE:
        return ByteBuff.wrap(copy(content, 0, content.length, false, rnd));
      case DIRECT:
        return ByteBuff.wrap(copy(content, 0, content.length, true, rnd));
      case POOLED:
        return pooled(content, 1024, rnd);
      case POOLED_HEAP_TAIL:
        return pooled(content, 1000, rnd);
      case TRIMMED:
        return trimmed(content, 1024, rnd);
      case UNEVEN:
        return uneven(content, rnd);
      default:
        throw new AssertionError(layout);
    }
  }

  /** Returns a copy of content in a randomly chosen layout. */
  private static ByteBuff randomLayout(byte[] content, Random rnd) {
    switch (rnd.nextInt(4)) {
      case 0:
        return newBuffer(Layout.values()[rnd.nextInt(Layout.values().length)], content, rnd);
      case 1:
        return pooled(content, 64 + rnd.nextInt(2048), rnd);
      case 2:
        return trimmed(content, 64 + rnd.nextInt(2048), rnd);
      default:
        return multi(content, rnd, randomSizes(content.length, rnd));
    }
  }

  /**
   * Returns a copy of content in direct buffers of bufSize bytes, the way ByteBuffAllocator hands
   * them out: the last one is only partly used, up to the limit of the returned buffer, unless
   * fewer than bufSize / 6 bytes are left, which go into a heap buffer instead.
   */
  private static ByteBuff pooled(byte[] content, int bufSize, Random rnd) {
    List<ByteBuffer> items = new ArrayList<>();
    int from = 0;
    while (content.length - from >= bufSize / 6) {
      // Past what is filled in, a pooled buffer holds whatever it held before.
      byte[] item = randomBytes(rnd, bufSize);
      int n = Math.min(bufSize, content.length - from);
      System.arraycopy(content, from, item, 0, n);
      items.add(copy(item, 0, bufSize, true, rnd));
      from += n;
    }
    if (from < content.length || items.isEmpty()) {
      items.add(copy(content, from, content.length - from, false, rnd));
    }
    return ByteBuff.wrap(items.toArray(new ByteBuffer[0])).limit(content.length);
  }

  /**
   * Returns a copy of content followed by random bytes in pooled buffers, limited to the content
   * the way the buffer of a block read from a file is limited to leave out its checksums.
   */
  private static ByteBuff trimmed(byte[] content, int bufSize, Random rnd) {
    byte[] withChecksums = randomBytes(rnd, content.length + 1 + rnd.nextInt(2 * bufSize));
    System.arraycopy(content, 0, withChecksums, 0, content.length);
    return pooled(withChecksums, bufSize, rnd).duplicate().limit(content.length);
  }

  /** Returns a copy of content in buffers of uneven sizes, a single byte and empty ones too. */
  private static ByteBuff uneven(byte[] content, Random rnd) {
    if (content.length == 0) {
      return multi(content, rnd, 0, 0);
    }
    int third = (content.length - 1) / 3;
    int half = (content.length - 1) / 2;
    return multi(content, rnd, 1, 0, third, half, 0, content.length - 1 - third - half);
  }

  /** Returns a copy of content in a MultiByteBuff of heap and direct buffers of the given sizes. */
  private static ByteBuff multi(byte[] content, Random rnd, int... sizes) {
    ByteBuffer[] items = new ByteBuffer[sizes.length];
    int from = 0;
    for (int i = 0; i < sizes.length; i++) {
      items[i] = copy(content, from, sizes[i], rnd.nextBoolean(), rnd);
      from += sizes[i];
    }
    assertEquals(content.length, from);
    return new MultiByteBuff(items);
  }

  /**
   * Returns a heap or direct ByteBuffer holding {@code src[from, from + length)} from index 0 up to
   * its limit. It is sliced out of a larger buffer, so it starts at a non-zero array offset or
   * address, and random bytes surround it, also between its limit and its capacity.
   */
  private static ByteBuffer copy(byte[] src, int from, int length, boolean direct, Random rnd) {
    int before = 1 + rnd.nextInt(16);
    int capacity = before + length + 1 + rnd.nextInt(16);
    ByteBuffer buf = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    buf.put(randomBytes(rnd, capacity)).position(before);
    buf.put(src, from, length).position(before);
    ByteBuffer slice = buf.slice();
    slice.limit(length);
    return slice;
  }

  /** Splits length into one to eight sizes at random points, which may give empty items. */
  private static int[] randomSizes(int length, Random rnd) {
    int[] cuts = new int[rnd.nextInt(8)];
    for (int i = 0; i < cuts.length; i++) {
      cuts[i] = rnd.nextInt(length + 1);
    }
    Arrays.sort(cuts);
    int[] sizes = new int[cuts.length + 1];
    int previous = 0;
    for (int i = 0; i < cuts.length; i++) {
      sizes[i] = cuts[i] - previous;
      previous = cuts[i];
    }
    sizes[cuts.length] = length - previous;
    return sizes;
  }

  private static byte[] randomBytes(Random rnd, int length) {
    byte[] bytes = new byte[length];
    rnd.nextBytes(bytes);
    return bytes;
  }

  /** Returns a copy of bytes with one byte changed, unless there are no bytes. */
  private static byte[] withOneByteChanged(byte[] bytes, Random rnd) {
    byte[] changed = bytes.clone();
    if (changed.length > 0) {
      changed[rnd.nextInt(changed.length)] += 1 + rnd.nextInt(255);
    }
    return changed;
  }

  /** Returns bytes cut short or followed by random bytes, to a random length. */
  private static byte[] withRandomLength(byte[] bytes, Random rnd) {
    byte[] resized = randomBytes(rnd, rnd.nextInt(bytes.length + 64));
    System.arraycopy(bytes, 0, resized, 0, Math.min(bytes.length, resized.length));
    return resized;
  }

  private static long randomOffset(Random rnd) {
    switch (rnd.nextInt(3)) {
      case 0:
        return -1;
      case 1:
        return rnd.nextInt(1 << 30);
      default:
        return rnd.nextLong();
    }
  }

  private static int randomNextBlockOnDiskSize(Random rnd) {
    return rnd.nextBoolean() ? -1 : HEADER_SIZE + rnd.nextInt(1 << 20);
  }
}
