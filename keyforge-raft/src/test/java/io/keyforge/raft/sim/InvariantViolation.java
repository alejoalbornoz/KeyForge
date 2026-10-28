package io.keyforge.raft.sim;

/** A safety (or end-of-run liveness) property of Raft does not hold. */
final class InvariantViolation extends RuntimeException {

    InvariantViolation(String message) {
        super(message);
    }
}
