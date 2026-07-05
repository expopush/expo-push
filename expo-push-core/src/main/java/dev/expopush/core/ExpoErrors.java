package dev.expopush.core;

import dev.expopush.api.NotificationOutcome;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushTicket;

/**
 * The single interpretation of Expo ticket/receipt errors. Every backend previously
 * carried its own copy of this mapping, which is exactly how the ACCEPTED-with-errorDetail
 * inconsistency happened.
 */
public final class ExpoErrors {

    /** Expo's error code for a token that is no longer valid. */
    public static final String DEVICE_NOT_REGISTERED = "DeviceNotRegistered";

    private ExpoErrors() {}

    /** Machine-readable error from a ticket, falling back to its message. */
    public static String errorOf(PushTicket ticket) {
        if (ticket == null) return "null ticket from Expo";
        var details = ticket.getDetails();
        if (details != null && details.getError() != null) {
            return details.getError();
        }
        return ticket.getMessage() != null ? ticket.getMessage() : "unknown ticket error";
    }

    /** Machine-readable error from a receipt, falling back to its message. */
    public static String errorOf(PushReceipt receipt) {
        var details = receipt.getDetails();
        if (details != null && details.getError() != null) {
            return details.getError();
        }
        return receipt.getMessage() != null ? receipt.getMessage() : "unknown receipt error";
    }

    /**
     * Maps an Expo error code to an outcome: {@code DeviceNotRegistered} means the token
     * is dead ({@link NotificationOutcome#REJECTED}, deactivate it); anything else is a
     * content/application problem ({@link NotificationOutcome#INVALID}, fix and retry).
     */
    public static NotificationOutcome outcomeFor(String error) {
        return DEVICE_NOT_REGISTERED.equals(error)
            ? NotificationOutcome.REJECTED
            : NotificationOutcome.INVALID;
    }
}
