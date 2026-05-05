package dev.expopush.core.security;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NoOpPayloadEncryptorTest {

    private final NoOpPayloadEncryptor encryptor = new NoOpPayloadEncryptor();

    @Test
    void encryptReturnsInputUnchanged() {
        assertThat(encryptor.encrypt("hello")).isEqualTo("hello");
    }

    @Test
    void decryptReturnsInputUnchanged() {
        assertThat(encryptor.decrypt("hello")).isEqualTo("hello");
    }

    @Test
    void encryptNullReturnsNull() {
        assertThat(encryptor.encrypt(null)).isNull();
    }

    @Test
    void decryptNullReturnsNull() {
        assertThat(encryptor.decrypt(null)).isNull();
    }

    @Test
    void encryptEmptyStringReturnsEmpty() {
        assertThat(encryptor.encrypt("")).isEmpty();
    }

    @Test
    void roundTripIsIdentity() {
        String input = "notification body with special chars: <>&\"'";
        assertThat(encryptor.decrypt(encryptor.encrypt(input))).isEqualTo(input);
    }
}
