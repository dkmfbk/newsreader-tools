package eu.fbk.nwrtools.processors;

import java.text.ParseException;

import javax.annotation.Nullable;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.Mapper;
import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.Reducer;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.vocab.VOID;

public class CountProcessor implements RDFProcessor, Mapper, Reducer {

    private static final URI DEFAULT_PREDICATE = VOID.TRIPLES; // not exactly correct...

    private static final URI DEFAULT_CONTEXT = null;

    private final URI predicate;

    private final URI context;

    static CountProcessor create(final String name, final String... args) throws ParseException {
        final Options options = Options.parse("p!|c!", args);
        final URI predicate = options.getOptionArg("p", URI.class, DEFAULT_PREDICATE);
        final URI context = options.getOptionArg("c", URI.class, DEFAULT_CONTEXT);
        return new CountProcessor(predicate, context);
    }

    public CountProcessor(final @Nullable URI predicate, @Nullable final URI context) {
        this.predicate = predicate;
        this.context = context;
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
        final URI subject = (URI) key;
        final Literal object = Statements.VALUE_FACTORY.createLiteral(statements.length);
        final Statement statement = this.context == null ? Statements.VALUE_FACTORY
                .createStatement(subject, this.predicate, object) : Statements.VALUE_FACTORY
                .createStatement(subject, this.predicate, object, this.context);
        handler.handleStatement(statement);
    }

}
