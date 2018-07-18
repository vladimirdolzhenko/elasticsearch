/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

public class RepairIndexTests extends IndexShardTestCase {

    public void testCorruptedIndex() throws Exception {
        final boolean primary = true;
        final IndexShard indexShard = newStartedShard(p ->
            newShard(p,
                Settings.builder()
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .build()), primary);

        // index some docs in several segments
        int numDocs = 0;
        for (int i = 0, attempts = randomIntBetween(5, 10); i < attempts; i++) {
            int numExtraDocs = between(10, 100);
            for (long j = 0; j < numExtraDocs; j++) {
                indexDoc(indexShard, "_doc", Long.toString(j), "{}");
            }
            numDocs += numExtraDocs;
            final FlushRequest flushRequest = new FlushRequest();
            flushRequest.force(true);
            flushRequest.waitIfOngoing(true);
            indexShard.flush(flushRequest);
        }

        final ShardPath shardPath = indexShard.shardPath();
        final Path indexPath = shardPath.getDataPath().resolve("index");

        final RepairIndexCommand ric = new RepairIndexCommand();
        final MockTerminal t = new MockTerminal();
        final OptionParser parser = ric.getParser();

        // Try running it before the shard is closed, it should flip out because it can't acquire the lock
        try {
            OptionSet options = parser.parse("-d", indexPath.toString(), "-fast");
            ric.execute(t, options, null);
            fail("expected the repair index command to fail not being able to acquire the lock");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to lock shard's directory"));
        }

        // close shard
        closeShards(indexShard);

        // corrupt files
        final Path[] filesToCorrupt =
            Files.walk(indexPath)
                .filter(p -> Files.isRegularFile(p) && IndexWriter.WRITE_LOCK_NAME.equals(p.getFileName().toString()) == false)
                .toArray(Path[]::new);
        CorruptionUtils.corruptFile(random(), filesToCorrupt);

        // open shard with the same location
        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(indexShard.routingEntry(),
            primary ? RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        );

        final IndexMetaData indexMetaData = IndexMetaData.builder(indexShard.indexSettings().getIndexMetaData())
            .settings(Settings.builder()
                .put(indexShard.indexSettings.getSettings())
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum"))
            .build();

        final IndexShard corruptedShard = newShard(shardRouting, shardPath, indexMetaData,
            null, indexShard.engineFactory,
            indexSettings -> {
                final ShardId shardId = shardPath.getShardId();
                final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
                    @Override
                    public Directory newDirectory() throws IOException {
                        final BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(shardPath.resolveIndex());
                        // index is corrupted - don't even try to check index on close - it fails
                        baseDirectoryWrapper.setCheckIndexOnClose(false);
                        return baseDirectoryWrapper;
                    }
                };
                return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
            },
            indexShard.getGlobalCheckpointSyncer(), EMPTY_EVENT_LISTENER);

        // it has to fail on start up due to index.shard.check_on_startup = checksum
        expectThrows(IndexShardRecoveryException.class, () -> newStartedShard(p -> corruptedShard, primary));

        closeShards(corruptedShard);

        // checking that lock has been released

        try (Directory dir = FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE);
             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            // Great, do nothing, we just wanted to obtain the lock
        }

        // index is closed, lock is released - run repair index
        t.addTextInput("y");
        OptionSet options = parser.parse("-d", indexPath.toString(), "-exorcise");
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        ric.execute(t, options, null);

        logger.info("--> output:\n{}", t.getOutput());

        // reopen shard, do checksum on start up

        final IndexShard newShard = newStartedShard(p ->
                newShard(shardRouting, shardPath, indexMetaData,
                    null, indexShard.engineFactory,
                    indexShard.getGlobalCheckpointSyncer(), EMPTY_EVENT_LISTENER),
            primary);

        assertThat("some docs should be lost due to corruption repair",
            getShardDocUIDs(newShard).size(), lessThan(numDocs));

        closeShards(newShard);

    }

}
