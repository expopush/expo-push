package dev.expopush.core.security;

/**
 * Strategy for encrypting and decrypting sensitive notification payloads
 * (title, body, metadata) before they are stored in persistent backends
 * like SQS or H2.
 */
public interface PayloadEncryptor {

    /**
     * Encrypts the given plaintext string.
     * If encryption is disabled or the input is null, returns it unchanged.
     */
    String encrypt(String plaintext);

    /**
     * Decrypts the given ciphertext string.
     * If encryption is disabled or the input is null, returns it unchanged.
     */
    String decrypt(String ciphertext);
}
