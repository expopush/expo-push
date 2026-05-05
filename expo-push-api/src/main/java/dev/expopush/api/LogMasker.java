package dev.expopush.api;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility for conditionally masking sensitive data in logs.
 *
 * <p>Controlled via the {@code expo.push.security.mask-sensitive-data-in-logs}
 * Spring property (default: true).
 */
public final class LogMasker {

    private static final AtomicBoolean maskingEnabled = new AtomicBoolean(true);

    private LogMasker() {}

    /**
     * Globally enables or disables masking.
     */
    public static void setMaskingEnabled(boolean enabled) {
        maskingEnabled.set(enabled);
    }

    /**
     * Masks the given value if masking is enabled.
     */
    public static String mask(String value) {
        if (value == null) return null;
        if (!maskingEnabled.get()) return value;
        
        if (value.length() <= 2) return "***";
        return value.charAt(0) + "..." + value.charAt(value.length() - 1) + " (length=" + value.length() + ")";
    }

    /**
     * Checks if masking is currently enabled.
     */
    public static boolean isMaskingEnabled() {
        return maskingEnabled.get();
    }
}
