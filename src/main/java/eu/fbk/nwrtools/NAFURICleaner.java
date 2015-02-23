package eu.fbk.nwrtools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import eu.fbk.nwrtools.util.CommandLine;
import eu.fbk.rdfpro.util.IO;

public class NAFURICleaner {

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
        return uri;
    }

    public void listFilesForFolder(final File folder) throws IOException, SAXException {
        if (folder.isDirectory()) {
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isDirectory()) {
                    listFilesForFolder(fileEntry);
                } else {
                    parse(fileEntry);

                }
            }
        } else {
            parse(folder);
        }
    }

    protected Node getNode(final String tagName, final NodeList nodes) {
        for (int x = 0; x < nodes.getLength(); x++) {
            final Node node = nodes.item(x);
            if (node.getNodeName().equalsIgnoreCase(tagName)) {
                return node;
            }
        }

        return null;
    }

    protected String getNodeAttr(final String attrName, final Node node) {
        final NamedNodeMap attrs = node.getAttributes();
        for (int y = 0; y < attrs.getLength(); y++) {
            final Node attr = attrs.item(y);
            if (attr.getNodeName().equalsIgnoreCase(attrName)) {
                return attr.getNodeValue();
            }
        }
        return "";
    }

    public void parse(final File file) throws IOException, SAXException {
        System.out.println("Opening... " + file);
        final DOMParser parser = new DOMParser();
        try (InputStream is = IO.buffer(IO.read(file.getAbsolutePath()))) {
            parser.parse(new InputSource(is));
        }
        final org.w3c.dom.Document doc = parser.getDocument();

        final Element root = doc.getDocumentElement();
        final NodeList rootlist = root.getChildNodes();
        final Node header = getNode("nafHeader", rootlist);
        final Element publicElem = (Element) getNode("public", header.getChildNodes());

        final String oldURI = getNodeAttr("uri", publicElem);
        final String newURI = cleanURI(oldURI).toString();
        publicElem.setAttribute("uri", newURI);

        // write the content into xml file
        try {
            final TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = null;
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            final DOMSource source = new DOMSource(doc);
            StreamResult result;
            if (file.getName().endsWith(".bz2")) {
                file.delete();
                final File xmlFile = new File(file.getCanonicalPath().replaceFirst(".bz2$", ""));
                result = new StreamResult(xmlFile);
                transformer.transform(source, result);
                final Runtime r = Runtime.getRuntime();
                final Process p = r.exec("bzip2 " + xmlFile);
                p.waitFor();

            } else {
                result = new StreamResult(file);
                transformer.transform(source, result);
            }

            System.out.println("Saved " + file);
        } catch (final Exception e) {
            System.out.println("URI failed: " + oldURI);
            System.out.println("Delete " + file);
            file.delete();
        }

    }

    public static void main(final String args[]) throws Exception {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("NAFURICleanr")
                    .withHeader(
                            "Clean bad document URIs inside NAF files supplied as arguments. "
                                    + "Accepts multiple arguments, including directories and bzipped files.")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

            final NAFURICleaner uriCleaner = new NAFURICleaner();
            for (final String arg : cmd.getArgs(String.class)) {
                uriCleaner.listFilesForFolder(new File(arg));
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    public static String detectEncoding(final InputStream in) throws IOException {
        String encoding = null;
        in.mark(400);
        int ignoreBytes = 0;
        boolean readEncoding = false;
        final byte[] buffer = new byte[400];
        int read = in.read(buffer, 0, 4);
        switch (buffer[0]) {
        case (byte) 0x00:
            if (buffer[1] == (byte) 0x00 && buffer[2] == (byte) 0xFE && buffer[3] == (byte) 0xFF) {
                ignoreBytes = 4;
                encoding = "UTF_32BE";
            } else if (buffer[1] == (byte) 0x00 && buffer[2] == (byte) 0x00
                    && buffer[3] == (byte) 0x3C) {
                encoding = "UTF_32BE";
                readEncoding = true;
            } else if (buffer[1] == (byte) 0x3C && buffer[2] == (byte) 0x00
                    && buffer[3] == (byte) 0x3F) {
                encoding = "UnicodeBigUnmarked";
                readEncoding = true;
            }
            break;
        case (byte) 0xFF:
            if (buffer[1] == (byte) 0xFE && buffer[2] == (byte) 0x00 && buffer[3] == (byte) 0x00) {
                ignoreBytes = 4;
                encoding = "UTF_32LE";
            } else if (buffer[1] == (byte) 0xFE) {
                ignoreBytes = 2;
                encoding = "UnicodeLittleUnmarked";
            }
            break;

        case (byte) 0x3C:
            readEncoding = true;
            if (buffer[1] == (byte) 0x00 && buffer[2] == (byte) 0x00 && buffer[3] == (byte) 0x00) {
                encoding = "UTF_32LE";
            } else if (buffer[1] == (byte) 0x00 && buffer[2] == (byte) 0x3F
                    && buffer[3] == (byte) 0x00) {
                encoding = "UnicodeLittleUnmarked";
            } else if (buffer[1] == (byte) 0x3F && buffer[2] == (byte) 0x78
                    && buffer[3] == (byte) 0x6D) {
                encoding = "ASCII";
            }
            break;
        case (byte) 0xFE:
            if (buffer[1] == (byte) 0xFF) {
                encoding = "UnicodeBigUnmarked";
                ignoreBytes = 2;
            }
            break;
        case (byte) 0xEF:
            if (buffer[1] == (byte) 0xBB && buffer[2] == (byte) 0xBF) {
                encoding = "UTF8";
                ignoreBytes = 3;
            }
            break;
        case (byte) 0x4C:
            if (buffer[1] == (byte) 0x6F && buffer[2] == (byte) 0xA7 && buffer[3] == (byte) 0x94) {
                encoding = "CP037";
            }
            break;
        }
        if (encoding == null) {
            encoding = System.getProperty("file.encoding");
            System.out.println(encoding);
        }
        if (readEncoding) {
            read = in.read(buffer, 4, buffer.length - 4);
            final Charset cs = Charset.forName(encoding);
            final String s = new String(buffer, 4, read, cs);
            final int pos = s.indexOf("encoding");
            if (pos == -1) {
                encoding = System.getProperty("file.encoding");
                System.out.println(encoding);
            } else {
                char delim;
                int start = s.indexOf(delim = '\'', pos);
                if (start == -1) {
                    start = s.indexOf(delim = '"', pos);
                }
                if (start == -1) {
                    throw new IllegalArgumentException();
                }
                final int end = s.indexOf(delim, start + 1);
                if (end == -1) {
                    throw new IllegalArgumentException();
                }
                encoding = s.substring(start + 1, end);
            }
        }

        in.reset();
        while (ignoreBytes-- > 0) {
            in.read();
        }
        return encoding;
    }

}
