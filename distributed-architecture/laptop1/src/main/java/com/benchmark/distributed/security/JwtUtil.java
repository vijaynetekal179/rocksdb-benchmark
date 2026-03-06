package com.benchmark.distributed.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.util.Date;

public class JwtUtil {
    // 256-bit secret key for HMAC-SHA256 signing
    private static final String SECRET = "benchmark-super-secret-jwt-key-32bytes!!";
    private static final SecretKey KEY = Keys.hmacShaKeyFor(SECRET.getBytes());
    private static final long EXPIRY_MS = 60 * 60 * 1000; // 1 hour

    /** Generate a signed JWT token for the given subject */
    public static String generateToken(String subject) {
        return Jwts.builder()
                .subject(subject)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + EXPIRY_MS))
                .signWith(KEY)
                .compact();
    }

    /** Validate a token and return its claims. Throws on invalid/expired token. */
    public static Claims validateToken(String token) {
        return Jwts.parser()
                .verifyWith(KEY)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
