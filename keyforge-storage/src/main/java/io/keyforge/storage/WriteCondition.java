package io.keyforge.storage;

/** When a conditional write takes effect. Expired keys count as absent. */
public enum WriteCondition {
    /** Always write. */
    ALWAYS,
    /** Write only if the key does not exist ({@code SET ... NX}). */
    IF_ABSENT,
    /** Write only if the key already exists ({@code SET ... XX}). */
    IF_PRESENT
}
