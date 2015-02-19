package eu.fbk.nwrtools;

import java.net.URI;
import java.nio.charset.Charset;

public class URICleaner {

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
     * @return the corresponding URI object
     * @throws IllegalArgumentException
     *             if the supplied string (after being cleaned) is still not valid (e.g., it does
     *             not contain a valid URI scheme) or represent a relative URI
     */
    public static URI cleanURI(final String string) throws IllegalArgumentException {

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
        for (int i = 0; i < string.length(); ++i) {
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
        return uri;
    }

}
