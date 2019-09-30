package ru.mail.polis.storage;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Row {
    public static final Comparator<Row> COMPARATOR = Comparator.comparing(Row::getKey).thenComparing(Row::getValue);
    private final ByteBuffer key;
    private final Value value;

    Row(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    @NotNull
    public static Row of(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        return new Row(key, Value.of(value));
    }

    @NotNull
    public static Row remove(@NotNull final ByteBuffer key) {
        return new Row(key, Value.remove());
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    public int getSizeBytes() {
        return Integer.BYTES + key.remaining() + value.getSizeBytes();
    }
}