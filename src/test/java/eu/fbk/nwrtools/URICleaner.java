package eu.fbk.nwrtools;

import java.net.URI;
import java.nio.charset.Charset;

import javax.annotation.Nullable;

public class URICleaner {

    /**
     * Check that the supplied string is a legal IRI (as per RFC 3987).
     *
     * @param string
     *            the IRI string to check
     * @throws IllegalArgumentException
     *             in case the IRI is illegal
     */
    public static void validateIRI(@Nullable final String string) throws IllegalArgumentException {

        // TODO: currently we check only the characters forming the IRI, not its structure

        // Ignore null input
        if (string == null) {
            return;
        }

        // Illegal characters should be percent encoded. Illegal IRI characters are all the
        // character that are not 'unreserved' (A-Z a-z 0-9 - . _ ~ 0xA0-0xD7FF 0xF900-0xFDCF
        // 0xFDF0-0xFFEF) or 'reserved' (! # $ % & ' ( ) * + , / : ; = ? @ [ ])

        for (int i = 0; i < string.length(); ++i) {
            final char c = string.charAt(i);
            if (c >= 'a' && c <= 'z' || c >= '?' && c <= '[' || c >= '&' && c <= ';' || c == '#'
                    || c == '$' || c == '!' || c == '=' || c == ']' || c == '_' || c == '~'
                    || c >= 0xA0 && c <= 0xD7FF || c >= 0xF900 && c <= 0xFDCF || c >= 0xFDF0
                    && c <= 0xFFEF) {
                // character is OK
            } else if (c == '%') {
                if (i >= string.length() - 2 || Character.digit(string.charAt(i + 1), 16) < 0
                        || Character.digit(string.charAt(i + 2), 16) < 0) {
                    throw new IllegalArgumentException("Illegal IRI '" + string
                            + "' (invalid percent encoding at index " + i + ")");
                }
            } else {
                throw new IllegalArgumentException("Illegal IRI '" + string
                        + "' (illegal character at index " + i + ")");
            }
        }
    }

    /**
     * Clean an illegal IRI string, trying to make it legal (as per RFC 3987).
     *
     * @param string
     *            the IRI string to clean
     * @return the cleaned IRI string (possibly the input unchanged) upon success
     * @throws IllegalArgumentException
     *             in case the supplied input cannot be transformed into a legal IRI
     */
    @Nullable
    public static String cleanIRI(@Nullable final String string) throws IllegalArgumentException {

        // TODO: we only replace illegal characters, but we should also check and fix the IRI
        // structure

        // We implement the cleaning suggestions provided at the following URL (section 'So what
        // exactly should I do?'), extended to deal with IRIs instead of URIs:
        // https://unspecified.wordpress.com/2012/02/12/how-do-you-escape-a-complete-uri/

        // Handle null input
        if (string == null) {
            return null;
        }

        // Illegal characters should be percent encoded. Illegal IRI characters are all the
        // character that are not 'unreserved' (A-Z a-z 0-9 - . _ ~ 0xA0-0xD7FF 0xF900-0xFDCF
        // 0xFDF0-0xFFEF) or 'reserved' (! # $ % & ' ( ) * + , / : ; = ? @ [ ])
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < string.length(); ++i) {
            final char c = string.charAt(i);
            if (c >= 'a' && c <= 'z' || c >= '?' && c <= '[' || c >= '&' && c <= ';' || c == '#'
                    || c == '$' || c == '!' || c == '=' || c == ']' || c == '_' || c == '~'
                    || c >= 0xA0 && c <= 0xD7FF || c >= 0xF900 && c <= 0xFDCF || c >= 0xFDF0
                    && c <= 0xFFEF) {
                builder.append(c);
            } else if (c == '%' && i < string.length() - 2
                    && Character.digit(string.charAt(i + 1), 16) >= 0
                    && Character.digit(string.charAt(i + 2), 16) >= 0) {
                builder.append('%'); // preserve valid percent encodings
            } else {
                builder.append('%').append(Character.forDigit(c / 16, 16))
                        .append(Character.forDigit(c % 16, 16));
            }
        }

        // Return the cleaned IRI (no Java validation as it is an IRI, not a URI)
        return builder.toString();
    }

    /**
     * Clean a possibly illegal URI string (in a way similar to what a browser does), returning
     * the corresponding cleaned {@code URI} object if successfull. A null result is returned for
     * a null input. Cleaning consists in (i) encode Unicode characters above U+0080 as UTF-8
     * octet sequences and (ii) percent-encode all resulting characters that are illegal as per
     * RFC 3896 (i.e., characters that are not 'reserved' or 'unreserved' according to the RFC).
     * Note that relative URIs are rejected by this method.
     *
     * @param string
     *            the input string
     * @return the resulting cleaned URI
     * @throws IllegalArgumentException
     *             if the supplied string (after being cleaned) is still not valid (e.g., it does
     *             not contain a valid URI scheme) or represent a relative URI
     */
    public static String cleanURI(final String string) throws IllegalArgumentException {

        // We implement the cleaning suggestions provided at the following URL (section 'So what
        // exactly should I do?'):
        // https://unspecified.wordpress.com/2012/02/12/how-do-you-escape-a-complete-uri/

        // Handle null input
        if (string == null) {
            return null;
        }

        // The input string should be first encoded as a sequence of UTF-8 bytes, so to deal with
        // Unicode chars properly (this encoding is a non-standard, common practice)
        final byte[] bytes = string.getBytes(Charset.forName("UTF-8"));

        // Then illegal characters should be percent encoded. Illegal characters are all the
        // character that are not 'unreserved' (A-Z a-z 0-9 - . _ ~) or 'reserved' (! # $ % & ' (
        // ) * + , / : ; = ? @ [ ])
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; ++i) {
            final int b = bytes[i] & 0xFF; // transform from signed to unsigned
            if (b >= 'a' && b <= 'z' || b >= '?' && b <= '[' || b >= '&' && b <= ';' || b == '#'
                    || b == '$' || b == '!' || b == '=' || b == ']' || b == '_' || b == '~') {
                builder.append((char) b);
            } else if (b == '%' && i < string.length() - 2
                    && Character.digit(string.charAt(i + 1), 16) >= 0
                    && Character.digit(string.charAt(i + 2), 16) >= 0) {
                builder.append('%'); // preserve valid percent encodings
            } else {
                builder.append('%').append(Character.forDigit(b / 16, 16))
                        .append(Character.forDigit(b % 16, 16));
            }
        }

        // Can now create an URI object, letting Java do further validation on the URI structure
        // (e.g., whether valid scheme, host, etc. have been provided)
        final URI uri = URI.create(builder.toString()).normalize();

        // We reject relative URIs, as they can cause problems downstream
        if (!uri.isAbsolute()) {
            throw new IllegalArgumentException("Not a valid absolute URI: " + uri);
        }

        // Can finally return the URI
        return uri.toString();
    }

}
