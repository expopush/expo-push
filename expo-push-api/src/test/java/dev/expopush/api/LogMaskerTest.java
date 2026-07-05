package dev.expopush.api;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogMaskerTest {

    @BeforeEach
    void enableMasking() {
        LogMasker.setMaskingEnabled(true);
    }

    @AfterEach
    void restoreMasking() {
        LogMasker.setMaskingEnabled(true);
    }

    // ─── mask() with masking enabled ─────────────────────────────────────────

    @Test
    void maskNullReturnsNull() {
        assertThat(LogMasker.mask(null)).isNull();
    }

    @Test
    void maskEmptyStringReturnsMask() {
        assertThat(LogMasker.mask("")).isEqualTo("***");
    }

    @Test
    void maskSingleCharReturnsMask() {
        assertThat(LogMasker.mask("x")).isEqualTo("***");
    }

    @Test
    void maskTwoCharReturnsMask() {
        assertThat(LogMasker.mask("ab")).isEqualTo("***");
    }

    @Test
    void maskNormalValueShowsFirstAndLastChar() {
        String result = LogMasker.mask("ExponentPushToken[abc]");
        assertThat(result)
            .startsWith("E")
            .endsWith("] (length=22)")
            .contains("...")
            .doesNotContain("ponentPushToke");
    }

    @Test
    void maskThreeCharShowsFirstAndLast() {
        String result = LogMasker.mask("abc");
        assertThat(result).startsWith("a").endsWith("c (length=3)");
    }

    // ─── mask() with masking disabled ────────────────────────────────────────

    @Test
    void maskDisabledReturnsValueUnchanged() {
        LogMasker.setMaskingEnabled(false);
        assertThat(LogMasker.mask("secret-token")).isEqualTo("secret-token");
    }

    @Test
    void maskDisabledNullStillReturnsNull() {
        LogMasker.setMaskingEnabled(false);
        assertThat(LogMasker.mask(null)).isNull();
    }

    // ─── isMaskingEnabled ────────────────────────────────────────────────────

    @Test
    void isMaskingEnabledReflectsSetterTrue() {
        LogMasker.setMaskingEnabled(true);
        assertThat(LogMasker.isMaskingEnabled()).isTrue();
    }

    @Test
    void isMaskingEnabledReflectsSetterFalse() {
        LogMasker.setMaskingEnabled(false);
        assertThat(LogMasker.isMaskingEnabled()).isFalse();
    }

    @Test
    void toggleMaskingOnAndOff() {
        LogMasker.setMaskingEnabled(false);
        assertThat(LogMasker.mask("hello")).isEqualTo("hello");

        LogMasker.setMaskingEnabled(true);
        assertThat(LogMasker.mask("hello")).startsWith("h");
        assertThat(LogMasker.mask("hello")).endsWith("o (length=5)");
    }

    // ─── Push tokens masked in toString ──────────────────────────────────────

    @Test
    void notificationCommandToStringMasksPushToken() {
        NotificationCommand cmd = new NotificationCommand(
            "ExponentPushToken[secret-device-token]", "title", "body",
            "corr-1", java.util.Map.of(), "h-1");

        assertThat(cmd.toString()).doesNotContain("secret-device-token");
    }

    @Test
    void notificationResultToStringMasksPushToken() {
        NotificationResult result = new NotificationResult(
            NotificationOutcome.ACCEPTED, "h-1", "corr-1",
            "ExponentPushToken[secret-device-token]", "title", "body",
            "ticket-1", null, java.util.Map.of());

        assertThat(result.toString()).doesNotContain("secret-device-token");
    }
}
