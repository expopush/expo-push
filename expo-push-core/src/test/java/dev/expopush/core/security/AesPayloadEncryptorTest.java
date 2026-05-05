package dev.expopush.core.security;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AesPayloadEncryptorTest {

    private static final String VALID_KEY_BASE64 = Base64.getEncoder().encodeToString(new byte[32]);

    private AesPayloadEncryptor encryptor() {
        return new AesPayloadEncryptor(VALID_KEY_BASE64);
    }

    // ─── Constructor validation ───────────────────────────────────────────────

    @Test
    void nullKeyThrowsIllegalArgument() {
        assertThatThrownBy(() -> new AesPayloadEncryptor(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not be blank");
    }

    @Test
    void blankKeyThrowsIllegalArgument() {
        assertThatThrownBy(() -> new AesPayloadEncryptor("   "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not be blank");
    }

    @Test
    void wrongKeyLengthThrowsIllegalArgument() {
        String shortKey = Base64.getEncoder().encodeToString(new byte[16]); // 16 bytes, not 32
        assertThatThrownBy(() -> new AesPayloadEncryptor(shortKey))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("256 bits");
    }

    @Test
    void invalidBase64KeyThrowsIllegalArgument() {
        assertThatThrownBy(() -> new AesPayloadEncryptor("not-valid-base64!!!"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid Base64");
    }

    // ─── Encrypt ─────────────────────────────────────────────────────────────

    @Test
    void encryptNullReturnsNull() {
        assertThat(encryptor().encrypt(null)).isNull();
    }

    @Test
    void encryptProducesNonNullBase64() {
        String result = encryptor().encrypt("hello world");
        assertThat(result).isNotNull().isNotBlank();
        // Should be valid Base64
        assertThat(Base64.getDecoder().decode(result)).isNotEmpty();
    }

    @Test
    void sameInputProducesDifferentCiphertexts() {
        // AES-GCM uses random IV per encryption
        AesPayloadEncryptor enc = encryptor();
        String c1 = enc.encrypt("same message");
        String c2 = enc.encrypt("same message");
        assertThat(c1).isNotEqualTo(c2);
    }

    // ─── Decrypt ─────────────────────────────────────────────────────────────

    @Test
    void decryptNullReturnsNull() {
        assertThat(encryptor().decrypt(null)).isNull();
    }

    @Test
    void encryptDecryptRoundTripShortString() {
        AesPayloadEncryptor enc = encryptor();
        String plaintext = "hi";
        assertThat(enc.decrypt(enc.encrypt(plaintext))).isEqualTo(plaintext);
    }

    @Test
    void encryptDecryptRoundTripLongString() {
        AesPayloadEncryptor enc = encryptor();
        String plaintext = "A".repeat(10_000);
        assertThat(enc.decrypt(enc.encrypt(plaintext))).isEqualTo(plaintext);
    }

    @Test
    void encryptDecryptRoundTripUnicode() {
        AesPayloadEncryptor enc = encryptor();
        String plaintext = "こんにちは 🌟 emoji test";
        assertThat(enc.decrypt(enc.encrypt(plaintext))).isEqualTo(plaintext);
    }

    @Test
    void encryptDecryptRoundTripEmptyString() {
        AesPayloadEncryptor enc = encryptor();
        assertThat(enc.decrypt(enc.encrypt(""))).isEmpty();
    }

    @Test
    void decryptWithWrongKeyThrowsRuntimeException() {
        AesPayloadEncryptor enc1 = encryptor();
        AesPayloadEncryptor enc2 = new AesPayloadEncryptor(
            Base64.getEncoder().encodeToString(new byte[]{
                1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
            }));

        String ciphertext = enc1.encrypt("secret message");

        assertThatThrownBy(() -> enc2.decrypt(ciphertext))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("decrypt");
    }

    @Test
    void decryptCorruptedCiphertextThrowsRuntimeException() {
        AesPayloadEncryptor enc = encryptor();
        assertThatThrownBy(() -> enc.decrypt("dGhpcyBpcyBub3QgdmFsaWQgY2lwaGVydGV4dA=="))
            .isInstanceOf(RuntimeException.class);
    }
}
