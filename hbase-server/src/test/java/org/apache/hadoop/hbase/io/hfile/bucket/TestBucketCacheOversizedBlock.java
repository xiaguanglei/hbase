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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.PrefetchExecutor;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A block larger than the largest bucket can never be written to the bucket cache. It must be
 * rejected before it is staged in the RAM cache and a writer queue, not left to fail its allocation
 * in the writer thread after it has been counted as a cached block.
 */
@Tag(IOTests.TAG)
@Tag(MediumTests.TAG)
public class TestBucketCacheOversizedBlock {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final long CAPACITY = 32 * 1024 * 1024;
  private static final int BLOCK_SIZE = 8 * 1024;
  /**
   * The only bucket size of the caches that blocks get written to, larger than the blocks of small
   * cells. Like configured bucket sizes, it is a multiple of 256, as cached block offsets must be.
   * The other caches get a bucket size relative to the length of the block they are offered.
   */
  private static final int BUCKET_SIZE = BLOCK_SIZE + 1024;

  private RecordingBucketCache cache;
  private HFileBlock block;
  private BlockCacheKey key;

  @BeforeEach
  public void setUp() {
    HFileBlockPair pair = CacheTestUtils.generateHFileBlocks(BLOCK_SIZE, 1)[0];
    block = pair.getBlock();
    key = pair.getBlockName();
  }

  @AfterEach
  public void tearDown() {
    if (cache != null) {
      cache.shutdown();
    }
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testOversizedBlockIsNotStaged() throws Exception {
    createCache(block.getSerializedLength() - 1, true);

    cache.cacheBlock(key, block);

    assertAll("a block over the largest bucket size must be rejected before it is staged",
      () -> assertEquals(0, cache.staged.size(), "block reached cacheBlockWithWaitInternal"),
      () -> assertFalse(cache.ramCache.containsKey(key), "block was put in the RAM cache"),
      () -> assertTrue(writerQueuesEmpty(), "block was put in a writer queue"),
      () -> assertEquals(0, cache.heapSize(), "block was added to the RAM cache heap size"),
      () -> assertEquals(0, cache.getBlockCount(), "block was added to the block count"),
      () -> assertNull(cache.getBlock(key, false, false, false), "block can be read back"),
      () -> assertEquals(1, failedInserts(), "block was not counted as a failed insert"),
      () -> assertEquals(0, allocationFailures(), "block was counted as an allocation failure"));
  }

  @Test
  public void testBlockOfLargestBucketSizeIsStaged() throws Exception {
    createCache(block.getSerializedLength(), true);

    cache.cacheBlock(key, block);

    assertAll("a block of exactly the largest bucket size fits, it must be staged as usual",
      () -> assertEquals(1, cache.staged.size(), "block did not reach cacheBlockWithWaitInternal"),
      () -> assertTrue(cache.ramCache.containsKey(key), "block was not put in the RAM cache"),
      () -> assertFalse(writerQueuesEmpty(), "block was not put in a writer queue"),
      () -> assertEquals(0, failedInserts(), "block was counted as a failed insert"));
  }

  @Test
  public void testDuplicateOversizedBlockIsNotCompared() throws Exception {
    createCache(block.getSerializedLength() - 1, true);

    cache.cacheBlock(key, block);
    cache.cacheBlock(key, block);

    assertAll("a rejected block leaves no staged copy to compare its duplicate with",
      () -> assertEquals(0, cache.replaceChecks.get(), "duplicate was compared with a copy"),
      () -> assertEquals(2, failedInserts(), "both inserts must be counted as failed"));
  }

  @Test
  public void testOversizedBlockDoesNotInflateBlockCount() throws Exception {
    createCache(block.getSerializedLength() - 1, false);

    cache.cacheBlock(key, block);
    waitForDrain();

    assertAll("a block over the largest bucket size must leave no trace once writers are idle",
      () -> assertEquals(0, cache.getBlockCount(), "block count stays above the cached blocks"),
      () -> assertTrue(cache.getBackingMap().isEmpty(), "block was cached"),
      () -> assertEquals(0, cache.heapSize(), "RAM cache heap size was not released"),
      () -> assertEquals(1, failedInserts(), "block was not counted as a failed insert"),
      () -> assertEquals(0, allocationFailures(), "writer thread failed to allocate the block"));
  }

  @Test
  public void testPersistentCacheRejectsBlockOversizedByCachedTime() throws Exception {
    // A persistent file cache also stores the time the block was cached at, in front of the block.
    useBlockOfLength(BUCKET_SIZE - Long.BYTES + 1);
    createPersistentCache();

    cache.cacheBlock(key, block);
    waitForDrain();

    assertAll("block and cached time are over the largest bucket size, block must be rejected",
      () -> assertEquals(0, cache.staged.size(), "block reached cacheBlockWithWaitInternal"),
      () -> assertEquals(1, failedInserts(), "block was not counted as a failed insert"),
      () -> assertFalse(cache.getBackingMap().containsKey(key), "block was cached"));
  }

  @Test
  public void testPersistentCacheStagesBlockFittingWithCachedTime() throws Exception {
    useBlockOfLength(BUCKET_SIZE - Long.BYTES);
    createPersistentCache();

    cache.cacheBlock(key, block);
    waitForDrain();

    assertAll("block and cached time fit the largest bucket size, block must be cached",
      () -> assertEquals(1, cache.staged.size(), "block did not reach cacheBlockWithWaitInternal"),
      () -> assertEquals(0, failedInserts(), "block was counted as a failed insert"),
      () -> assertTrue(cache.getBackingMap().containsKey(key), "block was not cached"));
  }

  @Test
  public void testPrefetchDoesNotStageOversizedBlock() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    FileSystem fs = HFileSystem.get(conf);
    createCache(BUCKET_SIZE, false);
    // Written without caching, so that only the prefetch offers blocks to the cache.
    Path path = writeHFile(conf, CacheConfig.DISABLED, fs);
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);

    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf, cache), true, conf);
    try {
      Waiter.waitFor(conf, PrefetchExecutor.getPrefetchDelay() + 10000,
        () -> PrefetchExecutor.isCompleted(path));
      waitForDrain();
      assertOversizedBlocksRejected();
    } finally {
      reader.close();
    }
  }

  @Test
  public void testCacheOnWriteDoesNotStageOversizedBlock() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    createCache(BUCKET_SIZE, false);

    writeHFile(conf, new CacheConfig(conf, cache), HFileSystem.get(conf));
    waitForDrain();

    assertOversizedBlocksRejected();
  }

  /** Checks what the cache did with the blocks of the HFile, one of which is oversized. */
  private void assertOversizedBlocksRejected() {
    List<BlockRef> oversized = select(cache.offered, ref -> ref.length > BUCKET_SIZE);
    List<BlockRef> fitting = select(cache.offered, ref -> ref.length <= BUCKET_SIZE);
    List<BlockRef> stagedOversized = select(cache.staged, ref -> ref.length > BUCKET_SIZE);
    List<BlockRef> cachedOversized = select(oversized, ref -> isCached(ref.key));
    List<BlockRef> uncachedFitting = select(fitting, ref -> !isCached(ref.key));
    int cachedBlocks = cache.getBackingMap().size();
    assertFalse(oversized.isEmpty(), "vacuous: no oversized block was offered to the cache");
    assertFalse(fitting.isEmpty(), "vacuous: no block that fits was offered to the cache");
    assertAll("oversized blocks must be rejected up front, the other blocks cached",
      () -> assertEquals(List.of(), stagedOversized, "oversized blocks were staged"),
      () -> assertEquals(oversized.size(), failedInserts(), "failed inserts != oversized blocks"),
      () -> assertEquals(List.of(), cachedOversized, "oversized blocks were cached"),
      () -> assertEquals(List.of(), uncachedFitting, "blocks that fit were not cached"),
      () -> assertEquals(cachedBlocks, cache.getBlockCount(), "block count != cached blocks"));
  }

  /** Writes an HFile of small cells with, in the middle, a cell far larger than the bucket. */
  private Path writeHFile(Configuration conf, CacheConfig cacheConf, FileSystem fs)
    throws IOException {
    Path path = new Path(TEST_UTIL.getDataTestDir(), "region/cf/hfile");
    HFileContext context = new HFileContextBuilder().withBlockSize(4 * 1024).build();
    HFile.WriterFactory writerFactory = HFile.getWriterFactory(conf, cacheConf);
    byte[] family = Bytes.toBytes("f");
    try (HFile.Writer writer = writerFactory.withPath(fs, path).withFileContext(context).create()) {
      for (int i = 0; i <= 100; i++) {
        byte[] row = Bytes.toBytes(String.format("row%05d", i));
        writer.append(new KeyValue(row, family, family, 1L, new byte[i == 50 ? 32 * 1024 : 100]));
      }
    }
    return path;
  }

  /** Replaces the block of the test by one of the given serialized length. */
  private void useBlockOfLength(int serializedLength) {
    HFileBlockPair pair =
      CacheTestUtils.generateHFileBlocks(serializedLength - HFileBlock.BLOCK_METADATA_SPACE, 1)[0];
    block = pair.getBlock();
    key = pair.getBlockName();
    assertEquals(serializedLength, block.getSerializedLength(), "unexpected block length");
  }

  private void createCache(int bucketSize, boolean stopWriters) throws Exception {
    cache = new RecordingBucketCache("offheap", bucketSize, null);
    if (stopWriters) {
      // Whatever is staged now stays in the RAM cache and the writer queues, to be observed.
      cache.stopWriterThreads();
    }
  }

  private void createPersistentCache() throws Exception {
    Path dir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(dir);
    cache = new RecordingBucketCache("file:" + dir + "/bucket.cache", BUCKET_SIZE,
      dir + "/bucket.persistence");
    assertTrue(cache.waitForCacheInitialization(10000), "the cache was not initialized");
    assertTrue(cache.isCachePersistent(), "the cache is not persistent");
  }

  private void waitForDrain() {
    long waited = Waiter.waitFor(TEST_UTIL.getConfiguration(), 10000, 100, false,
      () -> cache.ramCache.isEmpty() && writerQueuesEmpty());
    assertTrue(waited >= 0, "the writer threads did not drain the RAM cache within 10 s");
  }

  private boolean writerQueuesEmpty() {
    for (BlockingQueue<?> queue : cache.writerQueues) {
      if (!queue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private boolean isCached(BlockCacheKey blockKey) {
    return cache.getBackingMap().containsKey(blockKey);
  }

  private long failedInserts() {
    return cache.getStats().getFailedInserts();
  }

  private long allocationFailures() {
    return ((BucketCacheStats) cache.getStats()).getAllocationFailCount();
  }

  private static List<BlockRef> select(List<BlockRef> refs, Predicate<BlockRef> condition) {
    return refs.stream().filter(condition).collect(Collectors.toList());
  }

  /** A block offered to, or staged by, the cache, as it was when it got there. */
  private static final class BlockRef {
    private final BlockCacheKey key;
    private final int length;

    BlockRef(BlockCacheKey key, Cacheable block) {
      this.key = key;
      this.length = block.getSerializedLength();
    }

    @Override
    public String toString() {
      return key + " (" + length + " bytes)";
    }
  }

  /** Records the blocks offered to the cache, the ones it staged, and the duplicate checks. */
  private static final class RecordingBucketCache extends BucketCache {
    private final List<BlockRef> offered = new CopyOnWriteArrayList<>();
    private final List<BlockRef> staged = new CopyOnWriteArrayList<>();
    private final AtomicInteger replaceChecks = new AtomicInteger();

    RecordingBucketCache(String ioEngineName, int bucketSize, String persistencePath)
      throws IOException {
      super(ioEngineName, CAPACITY, BLOCK_SIZE, new int[] { bucketSize }, 1, 64, persistencePath);
    }

    @Override
    public void cacheBlockWithWait(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory,
      boolean wait) {
      offered.add(new BlockRef(cacheKey, cachedItem));
      super.cacheBlockWithWait(cacheKey, cachedItem, inMemory, wait);
    }

    @Override
    protected void cacheBlockWithWaitInternal(BlockCacheKey cacheKey, Cacheable cachedItem,
      boolean inMemory, boolean wait) {
      staged.add(new BlockRef(cacheKey, cachedItem));
      super.cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, wait);
    }

    @Override
    protected boolean shouldReplaceExistingCacheBlock(BlockCacheKey cacheKey, Cacheable newBlock) {
      replaceChecks.incrementAndGet();
      return super.shouldReplaceExistingCacheBlock(cacheKey, newBlock);
    }
  }
}
