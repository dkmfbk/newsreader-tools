package eu.fbk.nwrtools.util;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

public class RDFUtil {

    public static final ValueFactory FACTORY;

    private static final DatatypeFactory DATATYPE_FACTORY;

    static {
        FACTORY = ValueFactoryImpl.getInstance();
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    private static final Map<String, ImmutableSet<String>> NS_TO_PREFIX_MAP;

    private static final Map<String, String> PREFIX_TO_NS_MAP;

    static {
        try {
            System.setProperty("entityExpansionLimit", "" + Integer.MAX_VALUE);

            final ImmutableMap.Builder<String, ImmutableSet<String>> nsToPrefixBuilder;
            final ImmutableMap.Builder<String, String> prefixToNsBuilder;

            nsToPrefixBuilder = ImmutableMap.builder();
            prefixToNsBuilder = ImmutableMap.builder();

            for (final String line : Resources.readLines(RDFUtil.class.getResource("prefixes"),
                    Charsets.UTF_8)) {
                final Iterator<String> i = Splitter.on(' ').omitEmptyStrings().split(line)
                        .iterator();
                final String namespace = i.next();
                final ImmutableSet<String> prefixes = ImmutableSet.copyOf(i);
                nsToPrefixBuilder.put(namespace, prefixes);
                for (final String prefix : prefixes) {
                    prefixToNsBuilder.put(prefix, namespace);
                }
            }

            NS_TO_PREFIX_MAP = nsToPrefixBuilder.build();
            PREFIX_TO_NS_MAP = prefixToNsBuilder.build();

        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    @Nullable
    public static String prefixFor(final String namespace) {

        final ImmutableSet<String> result = NS_TO_PREFIX_MAP.get(namespace);
        if (result == null) {
            Preconditions.checkNotNull(namespace);
            return null;
        }
        return result.asList().get(0);
    }

    public static Set<String> prefixesFor(final String namespace) {

        final Set<String> result = NS_TO_PREFIX_MAP.get(namespace);
        if (result == null) {
            Preconditions.checkNotNull(namespace);
            return ImmutableSet.of();
        }
        return result;
    }

    @Nullable
    public static String namespaceFor(final String prefix) {

        final String result = PREFIX_TO_NS_MAP.get(prefix);
        if (result == null) {
            Preconditions.checkNotNull(prefix);
        }
        return result;
    }

    public static Value shortenValue(final Value value, final int threshold) {
        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            final URI datatype = literal.getDatatype();
            final String language = literal.getLanguage();
            final String label = ((Literal) value).getLabel();
            if (label.length() > threshold
                    && (datatype == null || datatype.equals(XMLSchema.STRING))) {
                int offset = threshold;
                for (int i = threshold; i >= 0; --i) {
                    if (Character.isWhitespace(label.charAt(i))) {
                        offset = i;
                        break;
                    }
                }
                final String newLabel = label.substring(0, offset) + "...";
                if (datatype != null) {
                    return FACTORY.createLiteral(newLabel, datatype);
                } else if (language != null) {
                    return FACTORY.createLiteral(newLabel, language);
                } else {
                    return FACTORY.createLiteral(newLabel);
                }
            }
        }
        return value;
    }

    public static String formatValue(final Value value) {
        final StringBuilder builder = new StringBuilder();
        formatValue(value, builder);
        return builder.toString();
    }

    public static void formatValue(final Value value, final StringBuilder builder) {

        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            builder.append('\"').append(literal.getLabel()).append('\"');
            final String language = literal.getLanguage();
            if (language != null) {
                builder.append('@').append(language);
            }
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                builder.append('^').append('^');
                formatValue(datatype, builder);
            }

        } else if (value instanceof BNode) {
            final BNode bnode = (BNode) value;
            builder.append('_').append(':').append(bnode.getID());

        } else if (value instanceof URI) {
            final URI uri = (URI) value;
            final String prefix = prefixFor(uri.getNamespace());
            if (prefix != null) {
                builder.append(prefix).append(":").append(uri.getLocalName());
            } else {
                builder.append('<').append(uri.stringValue()).append('>');
            }
        } else {
            throw new Error("Unknown value type (!): " + value);
        }
    }

    public static Value parseValue(final String string) {

        final int length = string.length();
        if (string.startsWith("\"") || string.startsWith("'")) {
            if (string.charAt(length - 1) == '"' || string.charAt(length - 1) == '\'') {
                return FACTORY.createLiteral(string.substring(1, length - 1));
            }
            int index = string.lastIndexOf("\"@");
            if (index == length - 4) {
                final String language = string.substring(index + 2);
                if (Character.isLetter(language.charAt(0))
                        && Character.isLetter(language.charAt(1))) {
                    return FACTORY.createLiteral(string.substring(1, index), language);
                }
            }
            index = string.lastIndexOf("\"^^");
            if (index > 0) {
                final String datatype = string.substring(index + 3);
                try {
                    final URI datatypeURI = (URI) parseValue(datatype);
                    return FACTORY.createLiteral(string.substring(1, index), datatypeURI);
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            throw new IllegalArgumentException("Invalid literal: " + string);

        } else if (string.startsWith("_:")) {
            return FACTORY.createBNode(string.substring(2));

        } else if (string.startsWith("<")) {
            return FACTORY.createURI(string.substring(1, length - 1));

        } else {
            final int index = string.indexOf(':');
            final String prefix = string.substring(0, index);
            final String localName = string.substring(index + 1);
            final String namespace = namespaceFor(prefix);
            if (namespace != null) {
                return FACTORY.createURI(namespace, localName);
            }
            throw new IllegalArgumentException("Unknown prefix for URI: " + string);
        }
    }

    /**
     * General conversion facility. This method attempts to convert a supplied {@code object} to
     * an instance of the class specified. If the input is null, null is returned. If conversion
     * is unsupported or fails, an exception is thrown. The following table lists the supported
     * conversions: <blockquote>
     * <table border="1">
     * <thead>
     * <tr>
     * <th>From classes (and sub-classes)</th>
     * <th>To classes (and super-classes)</th>
     * </tr>
     * </thead><tbody>
     * <tr>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean})</td>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string})</td>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string}), {@code URI} (as uri
     * string), {@code BNode} (as BNode ID), {@link Integer}, {@link Long}, {@link Double},
     * {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal}, {@link BigInteger},
     * {@link AtomicInteger}, {@link AtomicLong}, {@link Boolean}, {@link XMLGregorianCalendar},
     * {@link GregorianCalendar}, {@link Date} (via parsing), {@link Character} (length >= 1)</td>
     * </tr>
     * <tr>
     * <td>{@link Number}, {@link Literal} (any numeric {@code xsd:} type)</td>
     * <td>{@link Literal} (top-level numeric {@code xsd:} type), {@link Integer}, {@link Long},
     * {@link Double}, {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal},
     * {@link BigInteger}, {@link AtomicInteger}, {@link AtomicLong}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}, {@code xsd:date})</td>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link URI}</td>
     * <td>{@link URI}, {@link Record} (ID assigned), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link BNode}</td>
     * <td>{@link BNode}, {@link URI} (skolemization), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement}</td>
     * <td>{@link Statement}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Record}</td>
     * <td>{@link Record}, {@link URI} (ID extracted), {@link String}</td>
     * </tr>
     * </tbody>
     * </table>
     * </blockquote>
     * 
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or null if {@code object} was null
     * @throws IllegalArgumentException
     *             in case conversion fails or is unsupported for the {@code object} and class
     *             specified
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz)
            throws IllegalArgumentException {
        if (object == null) {
            Preconditions.checkNotNull(clazz);
            return null;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        final T result = (T) convertObject(object, clazz);
        if (result != null) {
            return result;
        }
        throw new IllegalArgumentException("Unsupported conversion of " + object + " to " + clazz);
    }

    /**
     * General conversion facility, with fall back to default value. This method operates as
     * {@link #convert(Object, Class)}, but in case the input is null or conversion is not
     * supported returns the specified default value.
     * 
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param defaultValue
     *            the default value to fall back to
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or the default value if {@code object} was null,
     *         conversion failed or is unsupported
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz,
            @Nullable final T defaultValue) {
        if (object == null) {
            Preconditions.checkNotNull(clazz);
            return defaultValue;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        try {
            final T result = (T) convertObject(object, clazz);
            return result != null ? result : defaultValue;
        } catch (final RuntimeException ex) {
            return defaultValue;
        }
    }

    @Nullable
    private static Object convertObject(final Object object, final Class<?> clazz) {
        if (object instanceof Literal) {
            return convertLiteral((Literal) object, clazz);
        } else if (object instanceof URI) {
            return convertURI((URI) object, clazz);
        } else if (object instanceof String) {
            return convertString((String) object, clazz);
        } else if (object instanceof Number) {
            return convertNumber((Number) object, clazz);
        } else if (object instanceof Boolean) {
            return convertBoolean((Boolean) object, clazz);
        } else if (object instanceof XMLGregorianCalendar) {
            return convertCalendar((XMLGregorianCalendar) object, clazz);
        } else if (object instanceof BNode) {
            return convertBNode((BNode) object, clazz);
        } else if (object instanceof Statement) {
            return convertStatement((Statement) object, clazz);
        } else if (object instanceof GregorianCalendar) {
            final XMLGregorianCalendar calendar = DATATYPE_FACTORY
                    .newXMLGregorianCalendar((GregorianCalendar) object);
            return clazz == XMLGregorianCalendar.class ? calendar : convertCalendar(calendar,
                    clazz);
        } else if (object instanceof Date) {
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime((Date) object);
            final XMLGregorianCalendar xmlCalendar = DATATYPE_FACTORY
                    .newXMLGregorianCalendar(calendar);
            return clazz == XMLGregorianCalendar.class ? xmlCalendar : convertCalendar(
                    xmlCalendar, clazz);
        } else if (object instanceof Enum<?>) {
            return convertEnum((Enum<?>) object, clazz);
        } else if (object instanceof File) {
            return convertFile((File) object, clazz);
        }
        return null;
    }

    @Nullable
    private static Object convertStatement(final Statement statement, final Class<?> clazz) {
        if (clazz.isAssignableFrom(String.class)) {
            return statement.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertLiteral(final Literal literal, final Class<?> clazz) {
        final URI datatype = literal.getDatatype();
        if (datatype == null || datatype.equals(XMLSchema.STRING)) {
            return convertString(literal.getLabel(), clazz);
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return convertBoolean(literal.booleanValue(), clazz);
        } else if (datatype.equals(XMLSchema.DATE) || datatype.equals(XMLSchema.DATETIME)) {
            return convertCalendar(literal.calendarValue(), clazz);
        } else if (datatype.equals(XMLSchema.INT)) {
            return convertNumber(literal.intValue(), clazz);
        } else if (datatype.equals(XMLSchema.LONG)) {
            return convertNumber(literal.longValue(), clazz);
        } else if (datatype.equals(XMLSchema.DOUBLE)) {
            return convertNumber(literal.doubleValue(), clazz);
        } else if (datatype.equals(XMLSchema.FLOAT)) {
            return convertNumber(literal.floatValue(), clazz);
        } else if (datatype.equals(XMLSchema.SHORT)) {
            return convertNumber(literal.shortValue(), clazz);
        } else if (datatype.equals(XMLSchema.BYTE)) {
            return convertNumber(literal.byteValue(), clazz);
        } else if (datatype.equals(XMLSchema.DECIMAL)) {
            return convertNumber(literal.decimalValue(), clazz);
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            return convertNumber(literal.integerValue(), clazz);
        } else if (datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.NON_POSITIVE_INTEGER)
                || datatype.equals(XMLSchema.NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.POSITIVE_INTEGER)) {
            return convertNumber(literal.integerValue(), clazz); // infrequent integer cases
        } else if (datatype.equals(XMLSchema.NORMALIZEDSTRING) || datatype.equals(XMLSchema.TOKEN)
                || datatype.equals(XMLSchema.NMTOKEN) || datatype.equals(XMLSchema.LANGUAGE)
                || datatype.equals(XMLSchema.NAME) || datatype.equals(XMLSchema.NCNAME)) {
            return convertString(literal.getLabel(), clazz); // infrequent string cases
        }
        return null;
    }

    @Nullable
    private static Object convertBoolean(final Boolean bool, final Class<?> clazz) {
        if (clazz == Boolean.class || clazz == boolean.class) {
            return bool;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return FACTORY.createLiteral(bool);
        } else if (clazz.isAssignableFrom(String.class)) {
            return bool.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertString(final String string, final Class<?> clazz) {
        if (clazz.isInstance(string)) {
            return string;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return FACTORY.createLiteral(string, XMLSchema.STRING);
        } else if (clazz.isAssignableFrom(URI.class)) {
            return FACTORY.createURI(string);
        } else if (clazz.isAssignableFrom(BNode.class)) {
            return FACTORY.createBNode(string.startsWith("_:") ? string.substring(2) : string);
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return Boolean.valueOf(string);
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf(string);
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(string);
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(string);
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(string);
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf(string);
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf(string);
        } else if (clazz == BigDecimal.class) {
            return new BigDecimal(string);
        } else if (clazz == BigInteger.class) {
            return new BigInteger(string);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(Integer.parseInt(string));
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(Long.parseLong(string));
        } else if (clazz == Date.class) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return DATATYPE_FACTORY.newXMLGregorianCalendar(fixed).toGregorianCalendar().getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return DATATYPE_FACTORY.newXMLGregorianCalendar(fixed).toGregorianCalendar();
        } else if (clazz.isAssignableFrom(XMLGregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return DATATYPE_FACTORY.newXMLGregorianCalendar(fixed);
        } else if (clazz == Character.class || clazz == char.class) {
            return string.isEmpty() ? null : string.charAt(0);
        } else if (clazz.isEnum()) {
            for (final Object constant : clazz.getEnumConstants()) {
                if (string.equalsIgnoreCase(((Enum<?>) constant).name())) {
                    return constant;
                }
            }
            throw new IllegalArgumentException("Illegal " + clazz.getSimpleName() + " constant: "
                    + string);
        } else if (clazz == File.class) {
            return new File(string);
        }
        return null;
    }

    @Nullable
    private static Object convertNumber(final Number number, final Class<?> clazz) {
        if (clazz.isAssignableFrom(Literal.class)) {
            if (number instanceof Integer || number instanceof AtomicInteger) {
                return FACTORY.createLiteral(number.intValue());
            } else if (number instanceof Long || number instanceof AtomicLong) {
                return FACTORY.createLiteral(number.longValue());
            } else if (number instanceof Double) {
                return FACTORY.createLiteral(number.doubleValue());
            } else if (number instanceof Float) {
                return FACTORY.createLiteral(number.floatValue());
            } else if (number instanceof Short) {
                return FACTORY.createLiteral(number.shortValue());
            } else if (number instanceof Byte) {
                return FACTORY.createLiteral(number.byteValue());
            } else if (number instanceof BigDecimal) {
                return FACTORY.createLiteral(number.toString(), XMLSchema.DECIMAL);
            } else if (number instanceof BigInteger) {
                return FACTORY.createLiteral(number.toString(), XMLSchema.INTEGER);
            }
        } else if (clazz.isAssignableFrom(String.class)) {
            return number.toString();
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf(number.intValue());
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(number.longValue());
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(number.doubleValue());
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(number.floatValue());
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf(number.shortValue());
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf(number.byteValue());
        } else if (clazz == BigDecimal.class) {
            return toBigDecimal(number);
        } else if (clazz == BigInteger.class) {
            return toBigInteger(number);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(number.intValue());
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(number.longValue());
        }
        return null;
    }

    @Nullable
    private static Object convertCalendar(final XMLGregorianCalendar calendar, //
            final Class<?> clazz) {
        if (clazz.isInstance(calendar)) {
            return calendar;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return FACTORY.createLiteral(calendar);
        } else if (clazz.isAssignableFrom(String.class)) {
            return calendar.toXMLFormat();
        } else if (clazz == Date.class) {
            return calendar.toGregorianCalendar().getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            return calendar.toGregorianCalendar();
        }
        return null;
    }

    @Nullable
    private static Object convertURI(final URI uri, final Class<?> clazz) {
        if (clazz.isInstance(uri)) {
            return uri;
        } else if (clazz.isAssignableFrom(String.class)) {
            return uri.stringValue();
        } else if (clazz == File.class && uri.stringValue().startsWith("file://")) {
            return new File(uri.stringValue().substring(7));
        }
        return null;
    }

    @Nullable
    private static Object convertBNode(final BNode bnode, final Class<?> clazz) {
        if (clazz.isInstance(bnode)) {
            return bnode;
        } else if (clazz.isAssignableFrom(URI.class)) {
            return FACTORY.createURI("bnode:" + bnode.getID());
        } else if (clazz.isAssignableFrom(String.class)) {
            return "_:" + bnode.getID();
        }
        return null;
    }

    @Nullable
    private static Object convertEnum(final Enum<?> constant, final Class<?> clazz) {
        if (clazz.isInstance(constant)) {
            return constant;
        } else if (clazz.isAssignableFrom(String.class)) {
            return constant.name();
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return FACTORY.createLiteral(constant.name(), XMLSchema.STRING);
        }
        return null;
    }

    @Nullable
    private static Object convertFile(final File file, final Class<?> clazz) {
        if (clazz.isInstance(file)) {
            return clazz.cast(file);
        } else if (clazz.isAssignableFrom(URI.class)) {
            return FACTORY.createURI("file://" + file.getAbsolutePath());
        } else if (clazz.isAssignableFrom(String.class)) {
            return file.getAbsolutePath();
        }
        return null;
    }

    private static BigDecimal toBigDecimal(final Number number) {
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        } else if (number instanceof Double || number instanceof Float) {
            final double value = number.doubleValue();
            return Double.isInfinite(value) || Double.isNaN(value) ? null : new BigDecimal(value);
        } else {
            return new BigDecimal(number.longValue());
        }
    }

    private static BigInteger toBigInteger(final Number number) {
        if (number instanceof BigInteger) {
            return (BigInteger) number;
        } else if (number instanceof BigDecimal) {
            return ((BigDecimal) number).toBigInteger();
        } else if (number instanceof Double || number instanceof Float) {
            return new BigDecimal(number.doubleValue()).toBigInteger();
        } else {
            return BigInteger.valueOf(number.longValue());
        }
    }

}
