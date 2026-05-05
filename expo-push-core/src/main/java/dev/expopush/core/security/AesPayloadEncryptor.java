package dev.expopush.core.security;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-GCM implementation of {@link PayloadEncryptor}.
 *
 * <p>Requires a 256-bit (32 byte) Base64-encoded key provided via
 * {@code expo.push.security.encryption-key}.
 */
public class AesPayloadEncryptor implements PayloadEncryptor {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int TAG_LENGTH_BIT = 128;
    private static final int IV_LENGTH_BYTE = 12;

    private final SecretKey secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    public AesPayloadEncryptor(String base64Key) {
        if (base64Key == null || base64Key.isBlank()) {
            throw new IllegalArgumentException("Encryption key must not be blank when payload encryption is enabled");
        }
        byte[] decodedKey;
        try {
            decodedKey = Base64.getDecoder().decode(base64Key);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Base64 encryption key", e);
        }
        if (decodedKey.length != 32) {
            throw new IllegalArgumentException("Encryption key must be 256 bits (32 bytes) after Base64 decoding");
        }
        this.secretKey = new SecretKeySpec(decodedKey, "AES");
    }

    @Override
    public String encrypt(String plaintext) {
        if (plaintext == null) return null;
        try {
            byte[] iv = new byte[IV_LENGTH_BYTE];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH_BIT, iv));

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + ciphertext.length);
            byteBuffer.put(iv);
            byteBuffer.put(ciphertext);

            return Base64.getEncoder().encodeToString(byteBuffer.array());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encrypt payload", e);
        }
    }

    @Override
    public String decrypt(String base64Ciphertext) {
        if (base64Ciphertext == null) return null;
        try {
            byte[] decoded = Base64.getDecoder().decode(base64Ciphertext);
            ByteBuffer byteBuffer = ByteBuffer.wrap(decoded);

            byte[] iv = new byte[IV_LENGTH_BYTE];
            byteBuffer.get(iv);

            byte[] ciphertext = new byte[byteBuffer.remaining()];
            byteBuffer.get(ciphertext);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH_BIT, iv));

            byte[] plaintext = cipher.doFinal(ciphertext);

            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decrypt payload — verify encryption key", e);
        }
    }
}
