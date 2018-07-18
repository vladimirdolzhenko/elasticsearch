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
import joptsimple.OptionSpec;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.Engine;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Repairs the index using the previous returned status
 */
public class RepairIndexCommand extends EnvironmentAwareCommand {

    private static final int MISCONFIGURATION = 1;
    private static final int FAILURE = 2;

    private final OptionSpec<String> indexFolder;
    private final OptionSpec<String> checksumsOnly;
    private final OptionSpec<String> crossCheckTermVectors;
    private final OptionSpec<String> exorcise;
    private final OptionSpec<String> segments;

    public RepairIndexCommand() {
        super("Repair of corrupted index");
        indexFolder = parser.acceptsAll(Arrays.asList("d", "dir"),
            "Index directory location on disk")
            .withRequiredArg()
            .required();

        checksumsOnly = parser.accepts("fast", "Do checksums only")
            .withOptionalArg();

        crossCheckTermVectors = parser.accepts("crossCheckTermVectors", "Do cross check term vectors")
            .withOptionalArg();

        exorcise = parser.accepts("exorcise", "Do exorcise")
            .withOptionalArg();

        segments = parser.accepts("segments", "Segments")
            .withOptionalArg();
    }

    // Visible for testing
    public OptionParser getParser() {
        return this.parser;
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool repairs the corrupted Lucene index");
    }

    @SuppressForbidden(reason = "Necessary to use the path passed in")
    private Path getIndexPath(OptionSet options) {
        return PathUtils.get(indexFolder.value(options), "", "");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        terminal.println("!   WARNING: Elasticsearch MUST be stopped before running this tool   !");
        terminal.println("! Please make a complete backup of your index before using this tool. !");
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        final PrintWriter writer = terminal.getWriter();
        final PrintStream printStream = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                writer.write(b);
            }
        }, false, "UTF-8");

        final Path path = getIndexPath(options);
        final boolean doChecksumsOnly = options.has(checksumsOnly);
        final boolean doCrossCheckTermVectors = options.has(crossCheckTermVectors);
        final boolean doExorcise = options.has(exorcise);

        if (doChecksumsOnly && doCrossCheckTermVectors) {
            throw new UserException(MISCONFIGURATION, "ERROR: cannot specify both -fast and -crossCheckTermVectors");
        }

        final List<String> onlySegments = segments.values(options);

        terminal.println("\nOpening index at " + path + "\n");
        Directory directory;
        try {
            directory = FSDirectory.open(path, NativeFSLockFactory.INSTANCE);
        } catch (Throwable t) {
            final String msg = "ERROR: could not open directory \"" + path + "\"; exiting";
            terminal.println(msg);
            throw new UserException(MISCONFIGURATION, msg);
        }

        try(Directory dir = directory) {

            CheckIndex.Status status;
            try (// Hold the lock open for the duration of the tool running
                 Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME);
                 CheckIndex checker = new CheckIndex(dir, writeLock)) {

                checker.setCrossCheckTermVectors(doCrossCheckTermVectors);
                checker.setChecksumsOnly(doChecksumsOnly);
                checker.setInfoStream(printStream, terminal.isPrintable(Terminal.Verbosity.VERBOSE));

                status = checker.checkIndex(onlySegments.isEmpty() ? null : onlySegments);

                if (status.missingSegments == false) {
                    if (status.clean == false) {
                        if (doExorcise == false) {
                            terminal.println("WARNING: would write new segments file, and " + status.totLoseDocCount
                                + " documents would be lost, if -exorcise were specified\n");
                        } else {
                            warnMessage(terminal, status);

                            terminal.println("Writing...");
                            checker.exorciseIndex(status);
                            terminal.println("OK");
                            terminal.println("Wrote new segments file \"" + status.segmentsFileName + "\"");
                        }
                    } else {
                        terminal.println("Index is clean");
                    }
                } else {
                    // TODO: drop it when creating new empty commit is fixed
                    // throw new UserException(FAILURE, "There are missing segments");
                }

            } catch (LockObtainFailedException lofe) {
                throw new UserException(MISCONFIGURATION,
                    "Failed to lock shard's directory at [" + path + "], is Elasticsearch still running?");
            }

            if (status.missingSegments) {
                terminal.println("There are missing segments");

                String[] files = directory.listAll();
                for (String file : files) {
                    if (file.equals(IndexWriter.WRITE_LOCK_NAME) == false) {
                        // directory.deleteFile(file);
                    }
                }

                // TODO: it is wrong historyUUID!
                final String historyUUID = UUIDs.randomBase64UUID();
                // commit the new empty history id
                final IndexWriterConfig iwc = new IndexWriterConfig(null)
                    .setCommitOnClose(true)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                try (IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
                    Map<String, String> newCommitData = new HashMap<>();
                    newCommitData.put(Engine.HISTORY_UUID_KEY, historyUUID);
                    indexWriter.setLiveCommitData(newCommitData.entrySet());
                    indexWriter.commit();
                }
            }
        }
    }

    /** Show a warning about losing data, asking for a confirmation */
    public static void warnMessage(Terminal terminal, CheckIndex.Status status) {
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        terminal.println(String.format(Locale.ROOT,
                  "!   WARNING: %18d documents will be lost.               !",
                   status.totLoseDocCount));
        terminal.println("!                                                                     !");
        terminal.println("!   WARNING:            YOU WILL LOSE DATA.                           !");
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        String text = terminal.readText("Continue and remove " + status.totLoseDocCount
            + " docs from the index ? [y/N] ");
        if (!text.equalsIgnoreCase("y")) {
            throw new ElasticsearchException("aborted by user");
        }
    }


}
