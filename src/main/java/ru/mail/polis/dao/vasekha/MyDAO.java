package ru.mail.polis.dao.vasekha;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.Record;
import ru.mail.polis.storage.MemTable;
import ru.mail.polis.storage.Row;
import ru.mail.polis.storage.SSTable;
import ru.mail.polis.storage.Time;
import ru.mail.polis.storage.Value;

public final class MyDAO implements DAO {
    private static final Logger log = LoggerFactory.getLogger(MyDAO.class);
    private static final String SUFFIX = ".db";
    private static final String SUFFIX_TMP = ".tmp";
    private final File folder;
    private final long flushThresholdBytes;
    private final MemTable memTable;
    private final Collection<SSTable> ssTables;

    /**
     * Creates persistence DAO.
     *
     * @param folder              the folder in which files will be written and read
     * @param flushThresholdBytes threshold of size of the memTable
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public MyDAO(@NotNull final File folder, final long flushThresholdBytes) throws IOException {
        this.folder = folder;
        this.flushThresholdBytes = flushThresholdBytes;
        memTable = new MemTable();
        ssTables = new ArrayList<>();
        Files.walkFileTree(folder.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(SUFFIX)) {
                    try {
                        ssTables.add(new SSTable(file));
                    } catch (IllegalArgumentException iae) {
                        log.error("Cannot create SSTable from " + file.getFileName() + ": " + iae.getMessage());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    private Iterator<Row> aliveRowIterator(@NotNull final ByteBuffer from) {
        final Collection<Iterator<Row>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable ssTable : ssTables) {
            iterators.add(ssTable.iterator(from));
        }
        final Iterator<Row> mergeSorted = Iterators.mergeSorted(iterators, Row.COMPARATOR);
        final Iterator<Row> collapsed = Iters.collapseEquals(mergeSorted, Row::getKey);
        return Iterators.filter(collapsed, row -> !row.getValue().isRemoved());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Row> alive = aliveRowIterator(from);
        return Iterators.transform(alive, row -> Record.of(row.getKey(), row.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.getSizeBytes() >= flushThresholdBytes) {
            flushMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.getSizeBytes() >= flushThresholdBytes) {
            flushMemTable();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSizeBytes() > 0) {
            flushMemTable();
        }
    }

    private void flushMemTable() throws IOException {
        final String tmpFileName = Time.getTimeNanos() + SUFFIX_TMP;
        memTable.flush(Path.of(folder.getAbsolutePath(), tmpFileName));
        final String finalFileName = Time.getTimeNanos() + SUFFIX;
        Files.move(
                Path.of(folder.getAbsolutePath(), tmpFileName),
                Path.of(folder.getAbsolutePath(), finalFileName),
                StandardCopyOption.ATOMIC_MOVE);
        ssTables.add(new SSTable(Path.of(folder.getAbsolutePath(), finalFileName)));
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Row> alive = aliveRowIterator(Value.EMPTY_BUFFER);
        final String tmpFileName = Time.getTimeNanos() + SUFFIX_TMP;
        SSTable.writeToFile(Path.of(folder.getAbsolutePath(), tmpFileName), alive);
        for (final SSTable ssTable : ssTables) {
            Files.delete(ssTable.getPath());
        }
        ssTables.clear();
        memTable.clear();
        final String finalFileName = Time.getTimeNanos() + SUFFIX;
        Files.move(
                Path.of(folder.getAbsolutePath(), tmpFileName),
                Path.of(folder.getAbsolutePath(), finalFileName),
                StandardCopyOption.ATOMIC_MOVE);
        ssTables.add(new SSTable(Path.of(folder.getAbsolutePath(), finalFileName)));
    }
}