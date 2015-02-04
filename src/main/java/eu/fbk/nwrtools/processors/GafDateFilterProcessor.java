package eu.fbk.nwrtools.processors;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.Mapper;
import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.Reducer;
import eu.fbk.rdfpro.util.Options;

public class GafDateFilterProcessor implements RDFProcessor, Mapper, Reducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafDateFilterProcessor.class);

    private static final String DEFAULT_GAF_DENOTED_BY = "http://groundedannotationframework.org/gaf#denotedBy";

    private static final Pattern PATTERN = Pattern.compile("(\\d{4})/(\\d{2})/(\\d{2})");

    private final Calendar startDate;

    private final Calendar endDate;

    private final URI denotedByURI;

    static GafDateFilterProcessor create(final String name, final String... args)
            throws ParseException {

        final Options options = Options.parse("a!|b!|u!", args);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");

        final String afterString = options.getOptionArg("a", String.class, "0/0/0");
        final Calendar afterDate = new GregorianCalendar();
        afterDate.setTime(formatter.parse(afterString));

        final String beforeString = options.getOptionArg("b", String.class, "10000/0/0");
        final Calendar beforeDate = new GregorianCalendar();
        beforeDate.setTime(formatter.parse(beforeString));

        final URI denotedByURI = new URIImpl(options.getOptionArg("u", String.class,
                DEFAULT_GAF_DENOTED_BY));

        return new GafDateFilterProcessor(afterDate, beforeDate, denotedByURI);
    }

    public GafDateFilterProcessor(final Calendar startDate, final Calendar endDate,
            final URI denotedByURI) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.denotedByURI = denotedByURI;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return RDFProcessors.mapReduce(this, this, true).wrap(handler);
    }

    @Override
    public Value[] map(final Statement statement) throws RDFHandlerException {
        return new Value[] { statement.getSubject() };
    }

    @Override
    public void reduce(final Value key, final Statement[] statements, final RDFHandler handler)
            throws RDFHandlerException {

        final BitSet discardBitmap = new BitSet(statements.length);

        boolean hasDenotedByInsideRange = false;
        boolean hasDenotedByOutsideRange = false;

        for (int i = 0; i < statements.length; ++i) {
            final Statement statement = statements[i];
            if (statement.getObject() instanceof URI
                    && statement.getPredicate().equals(this.denotedByURI)) {
                final String uri = statement.getObject().stringValue();
                try {
                    final Matcher matcher = PATTERN.matcher(uri);
                    if (matcher.find()) {
                        final int year = Integer.parseInt(matcher.group(1));
                        final int month = Integer.parseInt(matcher.group(2)) - 1; // 0 based
                        final int day = Integer.parseInt(matcher.group(3));
                        final GregorianCalendar date = new GregorianCalendar(year, month, day);
                        if (date.compareTo(this.startDate) >= 0
                                && date.compareTo(this.endDate) <= 0) {
                            hasDenotedByInsideRange = true;
                        } else {
                            hasDenotedByOutsideRange = true;
                            discardBitmap.set(i, true);
                        }
                    }
                } catch (final Throwable ex) {
                    LOGGER.warn("Could not extract and check date in mention URI: " + uri, ex);
                }
            }
        }

        if (!hasDenotedByOutsideRange || hasDenotedByInsideRange) {
            for (int i = 0; i < statements.length; ++i) {
                if (!discardBitmap.get(i)) {
                    handler.handleStatement(statements[i]);
                }
            }
        }
    }

}
