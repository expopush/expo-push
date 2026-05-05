package dev.expopush.core.security;

/**
 * Default {@link PayloadEncryptor} that performs no encryption.
 * Used when {@code expo.push.security.encrypt-payloads} is false.
 */
public class NoOpPayloadEncryptor implements PayloadEncryptor {

    @Override
    public String encrypt(String plaintext) {
        return plaintext;
    }

    @Override
    public String decrypt(String ciphertext) {
        return ciphertext;
    }
}
