package eu.fbk.nwrtools;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import eu.fbk.knowledgestore.Operation.Sparql;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.rdfpro.util.Statements;

public class QueryTestDriver {

    static abstract class Query {

        private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

        private static final ValueFactory FACTORY = ValueFactoryImpl.getInstance();

        private static final AtomicLong COUNTER = new AtomicLong(0);

        private final String name;

        private final Long timeout;

        private final Set<String> inputVariables;

        private final Set<String> outputVariables;

        Query(final String name, final Properties properties,
                final Iterable<String> inputVariables, final Iterable<String> outputVariables) {

            final String timeout = properties.getProperty("timeout");

            this.name = name;
            this.timeout = timeout != null ? Long.parseLong(timeout) : null;
            this.inputVariables = ImmutableSet.copyOf(inputVariables);
            this.outputVariables = ImmutableSet.copyOf(Iterables.concat( //
                    ImmutableSet.of("start", "time", "error"), inputVariables));
        }

        public static List<Query> create(final Properties properties) {

            final Map<String, Properties> map = Maps.newLinkedHashMap();
            for (final Object key : properties.keySet()) {
                final String keyString = key.toString();
                final int index = keyString.indexOf(".");
                if (index > 0) {
                    final String queryName = keyString.substring(0, index);
                    final String propertyName = keyString.substring(index + 1);
                    final String propertyValue = properties.getProperty(keyString);
                    Properties queryProperties = map.get(queryName);
                    if (queryProperties == null) {
                        queryProperties = new Properties();
                        map.put(queryName, queryProperties);
                    }
                    queryProperties.setProperty(propertyName, propertyValue);
                }
            }

            final List<Query> queries = Lists.newArrayList();
            for (final Map.Entry<String, Properties> entry : map.entrySet()) {
                final String queryName = entry.getKey();
                final Properties queryProperties = entry.getValue();
                final String queryType = queryProperties.getProperty("type");
                if ("download".equalsIgnoreCase(queryType)) {
                    queries.add(new DownloadQuery(queryName, queryProperties));
                } else if ("retrieve".equals(queryType)) {
                    queries.add(new RetrieveQuery(queryName, queryProperties));
                } else if ("count".equals(queryType)) {
                    queries.add(new CountQuery(queryName, queryProperties));
                } else if ("sparql".equalsIgnoreCase(queryType)) {
                    queries.add(new SparqlQuery(queryName, queryProperties));
                }
            }
            return queries;
        }

        public String getName() {
            return this.name;
        }

        public Long getTimeout() {
            return this.timeout;
        }

        public Set<String> getInputVariables() {
            return this.inputVariables;
        }

        public Set<String> getOutputVariables() {
            return this.outputVariables;
        }

        public BindingSet evaluate(final Session session, final BindingSet input) {

            final ValueFactory vf = ValueFactoryImpl.getInstance();
            final MapBindingSet output = new MapBindingSet();

            final long invocationId = COUNTER.incrementAndGet();
            MDC.put("query", invocationId + "-" + this.name);

            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Submitting [");
                for (final String variable : this.inputVariables) {
                    builder.append(" ");
                    builder.append(variable);
                    builder.append("=");
                    builder.append(output.getBinding(variable));
                }
                builder.append("]");
                LOGGER.debug(builder.toString());
            }

            String error = "";
            final long ts = System.currentTimeMillis();
            try {
                doEvaluate(session, input, output);
            } catch (final Throwable ex) {
                error = ex.getClass().getSimpleName() + " - "
                        + Strings.nullToEmpty(ex.getMessage());
                LOGGER.warn("Got exception", ex);
            }
            final long elapsed = System.currentTimeMillis() - ts;

            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("".equals(error) ? "Succeeded" : "Failed");
                builder.append(" after ");
                builder.append(elapsed);
                builder.append(" ms [");
                for (final String variable : this.outputVariables) {
                    builder.append(" ");
                    builder.append(variable);
                    builder.append("=");
                    builder.append(output.getBinding(variable));
                }
                builder.append("]");
                LOGGER.debug(builder.toString());
            }

            MDC.remove("query");

            output.addBinding("time", vf.createLiteral(elapsed));
            output.addBinding("start", vf.createLiteral(ts));
            output.addBinding("error", vf.createLiteral(error));

            return output;
        }

        abstract void doEvaluate(Session session, BindingSet input, MapBindingSet output)
                throws Throwable;

        private static class DownloadQuery extends Query {

            private final Template id;

            private final boolean caching;

            DownloadQuery(final String name, final Properties properties) {
                this(name, properties, new Template(properties.getProperty("id")));
            }

            private DownloadQuery(final String name, final Properties properties, final Template id) {
                super(name, properties, id.getVariables(), ImmutableList.of("size"));
                this.id = id;
                this.caching = "false".equalsIgnoreCase(properties.getProperty("caching"));
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final URI id = ValueFactoryImpl.getInstance()
                        .createURI(this.id.instantiate(input));

                long size = 0L;
                try (final Representation representation = session.download(id)
                        .caching(this.caching).timeout(getTimeout()).exec()) {
                    size = representation.writeToByteArray().length;

                } finally {
                    output.addBinding("size", FACTORY.createLiteral(size));
                }
            }

        }

        private static class RetrieveQuery extends Query {

            private final URI layer;

            @Nullable
            private final Template condition;

            @Nullable
            private final Long offset;

            @Nullable
            private final Long limit;

            @Nullable
            private final List<URI> properties;

            RetrieveQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("condition")));
            }

            private RetrieveQuery(final String name, final Properties properties,
                    @Nullable final Template condition) {

                super(name, properties, condition.getVariables(), ImmutableList.of("records"));

                final String offset = properties.getProperty("offset");
                final String limit = properties.getProperty("limit");

                List<URI> props = null;
                if (properties.containsKey("properties")) {
                    props = Lists.newArrayList();
                    for (final String token : Splitter.onPattern("[ ,;]").omitEmptyStrings()
                            .trimResults().split(properties.getProperty("properties"))) {
                        props.add((URI) Statements.parseValue(token));
                    }
                }

                this.layer = (URI) Statements.parseValue(properties.getProperty("layer"));
                this.condition = condition;
                this.offset = offset == null ? null : Long.parseLong(offset);
                this.limit = limit == null ? null : Long.parseLong(limit);
                this.properties = props;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final String condition = Strings.nullToEmpty(this.condition.instantiate(input));

                long numResults = 0L;
                try {
                    numResults = session.retrieve(this.layer).condition(condition)
                            .offset(this.offset).limit(this.limit).properties(this.properties)
                            .exec().count();
                } finally {
                    output.addBinding("records", FACTORY.createLiteral(numResults));
                }
            }

        }

        private static class CountQuery extends Query {

            private final URI layer;

            @Nullable
            private final Template condition;

            CountQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("condition")));
            }

            private CountQuery(final String name, final Properties properties,
                    @Nullable final Template condition) {

                super(name, properties, condition.getVariables(), ImmutableList.of("records"));

                this.layer = (URI) Statements.parseValue(properties.getProperty("layer"));
                this.condition = condition;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final String condition = Strings.nullToEmpty(this.condition.instantiate(input));
                long numResults = 0L;
                try {
                    numResults = session.count(this.layer).condition(condition).exec();
                } finally {
                    output.addBinding("records", FACTORY.createLiteral(numResults));
                }
            }

        }

        private static final class SparqlQuery extends Query {

            private final Template query;

            private final String form;

            SparqlQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("query")));
            }

            private SparqlQuery(final String name, final Properties properties,
                    final Template query) {
                super(name, properties, query.getVariables(), ImmutableList.of("results"));
                this.query = query;
                this.form = detectQueryForm(query.getText());
            }

            private static String detectQueryForm(final String query) {

                final int length = query.length();

                int start = 0;
                while (start < length) {
                    final char ch = query.charAt(start);
                    if (ch == '#') { // comment
                        while (start < length && query.charAt(start) != '\n') {
                            ++start;
                        }
                    } else if (ch == 'p' || ch == 'b' || ch == 'P' || ch == 'B') { // prefix/base
                        while (start < length && query.charAt(start) != '>') {
                            ++start;
                        }
                    } else if (!Character.isWhitespace(ch)) { // found
                        break;
                    }
                    ++start;
                }

                for (int i = start; i < query.length(); ++i) {
                    final char ch = query.charAt(i);
                    if (Character.isWhitespace(ch)) {
                        final String form = query.substring(start, i).toLowerCase();
                        if (!"select".equals(form) && !"construct".equals(form)
                                && !"describe".equals(form) && !"ask".equals(form)) {
                            throw new IllegalArgumentException("Unknown query form: " + form);
                        }
                        return form;
                    }
                }

                throw new IllegalArgumentException("Cannot detect query form");
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                long numResults = 0;
                final Sparql operation = session.sparql(this.query.instantiate(input)).timeout(
                        getTimeout());

                try {
                    switch (this.form) {
                    case "select":
                        numResults = operation.execTuples().count();
                        break;
                    case "construct":
                    case "describe":
                        numResults = operation.execTriples().count();
                        break;
                    case "ask":
                        operation.execBoolean();
                        numResults = 1;
                        break;
                    default:
                        throw new Error();
                    }
                } finally {
                    output.addBinding("results", FACTORY.createLiteral(numResults));
                }
            }

        }

        private static final class Template {

            private static final Pattern PATTERN = Pattern.compile("${([^}]+)}");

            private static Template EMPTY = new Template("");

            private final String text;

            private final String[] placeholderVariables;

            private final Set<String> variables;

            private Template(final String string) {
                Preconditions.checkNotNull(string);
                final List<String> variables = Lists.newArrayList();
                final StringBuilder builder = new StringBuilder();
                final Matcher matcher = PATTERN.matcher(string);
                int offset = 0;
                while (matcher.find()) {
                    builder.append(string.substring(offset, matcher.start()).replace("%", "%%"));
                    builder.append("%s");
                    variables.add(matcher.group(1));
                    offset = matcher.end();
                }
                builder.append(string.substring(offset).replace("%", "%%"));
                this.text = builder.toString();
                this.placeholderVariables = variables.toArray(new String[variables.size()]);
                this.variables = ImmutableSet.copyOf(variables);
            }

            static Template forString(@Nullable final String string) {
                return string == null ? EMPTY : new Template(string);
            }

            String getText() {
                return this.text;
            }

            Set<String> getVariables() {
                return this.variables;
            }

            String instantiate(final BindingSet bindings) {
                final Object[] placeholderValues = new String[this.placeholderVariables.length];
                for (int i = 0; i < placeholderValues.length; ++i) {
                    final Value value = bindings.getValue(this.placeholderVariables[i]);
                    placeholderValues[i] = Data.toString(value, null);
                }
                return String.format(this.text, placeholderValues);
            }

        }

    }

}
