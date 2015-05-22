package eu.fbk.nwrtools;

import java.io.File;
import java.util.*;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Stream;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.nwrtools.util.CommandLine;
import eu.fbk.rdfpro.util.Statements;

public class LinkingAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkingAnalyzer.class);

        private static final String TAXONOMY_QUERY = "" + //
            "  SELECT ?super ?sub\n" + //
            "  WHERE {\n" + //
            "    ?sub a owl:Class .\n" + //
            "    ?super a owl:Class .\n" + //
            "    ?sub rdfs:subClassOf ?super .\n" + //
            "    FILTER (?super != ?sub)\n" + //
            "    FILTER (STRSTARTS(STR(?super), \"http://dbpedia.org/ontology/\"))\n" + //
            "    FILTER (STRSTARTS(STR(?sub), \"http://dbpedia.org/ontology/\"))\n" + //
            "    FILTER NOT EXISTS {\n" + //
            "      ?class rdfs:subClassOf ?super .\n" + //
            "      ?sub rdfs:subClassOf ?class .\n" + //
            "      FILTER (?class != ?super && ?class != ?sub)\n" + //
            "      FILTER (STRSTARTS(STR(?class), \"http://dbpedia.org/ontology/\"))\n" + //
            "    }\n" + //
            "  }\n" + //
            "  ORDER BY ?super ?sub";


    private static final String INSTANCES_QUERY = "" + //
            "  SELECT ?type\n" + //
            "         (COUNT(DISTINCT ?l) AS ?linkedEntities)\n" + //
            "         (COUNT(DISTINCT ?e) AS ?totalEntities)\n" + //
            "         (COUNT(DISTINCT ?d) AS ?documents)\n" + //
            "  WHERE {\n" + //
            "    {\n" + //
            "      SELECT DISTINCT ?type WHERE {\n" + //
            "        ?type a owl:Class .\n" + //
            "        FILTER (STRSTARTS(STR(?type), \"http://dbpedia.org/ontology/\"))\n" + //
            "      }\n" + //
            "    }\n" + //
            "    ?e a ?type .\n" + //
            "    OPTIONAL {\n" + //
            "      ?e gaf:denotedBy ?m .\n" + //
            "      BIND (?e AS ?l)\n" + //
            "      BIND (MD5(STRBEFORE(STR(?m), \"#\")) AS ?d)\n" + //
            "    }\n" + //
            "  }\n" + //
            "  GROUP BY ?type\n" + //
            "  HAVING (COUNT(DISTINCT ?d) >= $$)\n" + //
            "  ORDER BY DESC(?documents)";


    private static final String TAXONOMY_QUERY_ANN = "" + //
            "  SELECT ?super ?sub\n" + //
            "  WHERE {\n" + //
            "    ?sub rdfs:subClassOf ?super .\n" + //
            "    FILTER (?super != ?sub)\n" + //
            "    ?super nwr:isClassDefinedBy dbo: .\n" + //
            "    ?sub nwr:isClassDefinedBy dbo: .\n" + //
            "    FILTER NOT EXISTS {\n" + //
            "      ?class rdfs:subClassOf ?super .\n" + //
            "      ?sub rdfs:subClassOf ?class .\n" + //
            "      FILTER (?class != ?super && ?class != ?sub)\n" + //
            "      ?class nwr:isClassDefinedBy dbo: .\n" + //
            "    }\n" + //
            "  }\n" + //
            "  ORDER BY ?super ?sub";

    private static final String INSTANCES_QUERY_ANN = "" + //
            "  SELECT ?type\n" + //
            "         (COUNT(DISTINCT ?l) AS ?linkedEntities)\n" + //
            "         (COUNT(DISTINCT ?e) AS ?totalEntities)\n" + //
            "         (COUNT(DISTINCT ?d) AS ?documents)\n" + //
            "  WHERE {\n" + //
            "    {\n" + //
            "      SELECT DISTINCT ?type WHERE {\n" + //
            "        ?type  nwr:isClassDefinedBy dbo: .\n" + //
            "      }\n" + //
            "    }\n" + //
            "    ?e a ?type .\n" + //
            "    OPTIONAL {\n" + //
            "      ?e gaf:denotedBy ?m .\n" + //
            "      BIND (?e AS ?l)\n" + //
            "      BIND (MD5(STRBEFORE(STR(?m), \"#\")) AS ?d)\n" + //
            "    }\n" + //
            "  }\n" + //
            "  GROUP BY ?type\n" + //
            "  HAVING (COUNT(DISTINCT ?d) >= $$)\n" + //
            "  ORDER BY DESC(?documents)";

    private static final String COUNT_DOCUMENTS = "" + //
            "  SELECT (COUNT (DISTINCT ?doc) as ?num_doc)\n" + //
            "  WHERE{\n" +//
            "     ?a gaf:denotedBy ?m\n" +//
            "     BIND (strbefore(str(?m),\"#char\") as ?doc)\n" + //
            "  }";//


    private final Session session;
//    private final File occurrencesFile;
//
//
//    private final File taxonomyFile;

    private long timeoutMs;
    private int totalDocuments;
    private final int granularity;

    private final Map<URI, Type> types;

    public LinkingAnalyzer(final Session session, final int granularity, final Integer timeoutSec) {

        this.session = session;
        this.timeoutMs = timeoutSec*1000L;
//        this.occurrencesFile = Preconditions.checkNotNull(occurrencesFile);
//        this.taxonomyFile = Preconditions.checkNotNull(taxonomyFile);
        this.totalDocuments = 0;
        this.granularity = granularity;
        this.types = Maps.newHashMap();
    }

    public String analyze() {
        try {
            countDocuments();
            collectTypeOccurrences();
            collectTypeTaxonomy();
            //System.out.println(this.types.toString());
            return formatReport();

        } catch (final Throwable ex) {
            throw Throwables.propagate(ex);
        }
    }


    private void countDocuments() throws Throwable {
        Stream<BindingSet> stream = session.sparql(COUNT_DOCUMENTS).timeout(this.timeoutMs).execTuples();
        List<BindingSet> tuples = stream.toList();
        //@SuppressWarnings("unchecked");
        //List<String> variables = stream.getProperty("variables", List.class);

        for (BindingSet tuple : tuples) {
            try {
                this.totalDocuments = ((Literal) tuple.getValue("num_doc")).intValue();
                System.out.println("DOCUMENTS = " + this.totalDocuments);
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring instances line (" + ex.getMessage() + "): " + tuple.toString());
            }
        }
    }

    private void collectTypeOccurrences() throws Throwable {

        //call query for instances

        Stream<BindingSet> stream = session.sparql(INSTANCES_QUERY,this.granularity).timeout(this.timeoutMs).execTuples();
        List<BindingSet> tuples = stream.toList();
        @SuppressWarnings("unchecked")
        List<String> variables = stream.getProperty("variables", List.class);

        for (BindingSet tuple : tuples) {
            try {
                final URI uri = new URIImpl(tuple.getValue("type").stringValue());
                System.out.println(uri);
                final int entities = ((Literal) tuple.getValue("linkedEntities")).intValue();
                System.out.println(entities);
                final int totalEntities = ((Literal) tuple.getValue("totalEntities")).intValue();
                System.out.println(totalEntities);
                final int documents = ((Literal) tuple.getValue("documents")).intValue();
                System.out.println(documents);
                Type type = this.types.get(uri);
                if (type == null) {
                    type = new Type(uri, entities, totalEntities, documents);
                    this.types.put(uri, type);
                }
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring instances line (" + ex.getMessage() + "): " + tuple.toString());
            }
        }
    }

    private Multimap<URI, URI> collectTypeTaxonomy() throws Throwable {
        final Multimap<URI, URI> result = HashMultimap.create();
        //call query for taxonomy

        Stream<BindingSet> stream = session.sparql(TAXONOMY_QUERY).timeout(this.timeoutMs).execTuples();
        List<BindingSet> tuples = stream.toList();
        @SuppressWarnings("unchecked")
        List<String> variables = stream.getProperty("variables", List.class);

        for (BindingSet tuple : tuples) {
            try {
                //System.out.println(tuple.getBindingNames());
                final URI parent = new URIImpl(tuple.getValue("super").stringValue());
                final URI child = new URIImpl(tuple.getValue("sub").stringValue());
                System.out.println(parent+" "+child);
                final Type parentType = this.types.get(parent);
                final Type childType = this.types.get(child);
                if (parentType != null && childType != null) {
                    parentType.children.add(child);
                    childType.parents.add(parent);
                }
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring taxonomy line (" + ex.getMessage() + "): " + tuple.toString());
            }
        }

        return result;
    }

    private String[] parseLine(final String line) {
        final String[] tokens = line.split("\\s*[,;\\s]\\s*");
        for (int i = 0; i < tokens.length; ++i) {
            final char c = tokens[i].charAt(0);
            if (c == '\'' || c == '\"' || c == '<' || c == '_') {
                tokens[i] = Statements.parseValue(tokens[i]).stringValue();
            }
        }
        return tokens;
    }

    private String formatReport() {

        final List<Type> roots = Lists.newArrayList();
        for (final Type type : this.types.values()) {
            if (type.parents.isEmpty()) {
                roots.add(type);
            }
        }
        Collections.sort(roots);

        final StringBuilder builder = new StringBuilder();
        builder.append("<html>\n<head>\n");
        builder.append("<style type=\"text/css\">\n");
        builder.append("table { border-collapse: collapse }\n");
        builder.append("td, th { padding-left: 1em; padding-right: 1em; text-align: left; }\n");
        builder.append("th { border-bottom: 1px solid black }\n");
        builder.append(".l0 { font-size: 100% }\n");
        builder.append(".l1 { font-size: 90%  }\n");
        builder.append(".l2 { font-size: 80%  }\n");
        builder.append(".l3 { font-size: 70%  }\n");
        builder.append(".l4 { font-size: 60%  }\n");
        builder.append(".l5 { font-size: 60%  }\n");
        builder.append(".l6 { font-size: 60%  }\n");
        builder.append(".l7 { font-size: 60%  }\n");
        builder.append(".l8 { font-size: 60%  }\n");
        builder.append(".l9 { font-size: 60%  }\n");
        builder.append("</style>\n");
        builder.append("</head>\n<body>\n");

        // for (final Type root : roots) {
        // formatReportHelper(builder, root, 0);
        // }

        builder.append("<table>\n<thead>\n");
        builder.append("<tr><th>type</th><th># linked<br/>instances</th>"
                + "<th>% type<br/>instances</th><th># docs</th><th>% total<br/>docs</th></tr>\n");
        builder.append("</thead>\n<tbody>\n");
        for (final Type root : roots) {
            formatReportHelper(builder, root, 0);
        }
        builder.append("</tbody>\n</table>\n");

        builder.append("</body>\n</html>\n");
        return builder.toString();
    }

    private void formatReportHelper(final StringBuilder builder, final Type type, final int indent) {

        final List<Type> children = Lists.newArrayList();
        for (final URI uri : type.children) {
            final Type child = this.types.get(uri);
            System.out.println(child.uri.toString());
            if (child != null && child.entities <= type.entities) {
                children.add(child);
            }
        }

        builder.append("<tr class=\"l" + indent + "\"><td>");
        for (int i = 0; i < indent; ++i) {
            builder.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
        }
        builder.append(type.uri.getLocalName());
        builder.append("</td>");
        builder.append("<td>").append(type.entities).append("</td>");
        builder.append("<td>").append(type.entities * 1000 / type.totalEntities / 10.0)
                .append("%</td>");
        builder.append("<td>").append(type.documents).append("</td>");
        builder.append("<td>").append(type.documents * 1000 / this.totalDocuments / 10.0)
                .append("%</td>");
        builder.append("</tr>\n");

        if (!children.isEmpty()) {
            for (final Type child : Ordering.natural().sortedCopy(children)) {
                formatReportHelper(builder, child, indent + 1);
            }
        }
    }

    public static void main(final String[] args) {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("LinkingAnalyzer")
                    .withHeader(
                            "Generate an HTML report with statistics on linking of named entity to DBpedia")
                    .withOption("s", "server", "the URL of the KS instance", "URL",
                            CommandLine.Type.STRING, true, false, true)
                    .withOption("u", "username", "the KS username (if required)", "USER",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("p", "password", "the KS password (if required)", "PWD",
                            CommandLine.Type.STRING, true, false, false)
//                    .withOption("d", "documents",
//                            "the total number of documents (for computing percentages)", "NUM",
//                            CommandLine.Type.INTEGER, true, false, true)
                    .withOption("g", "granularity",
                            "the granularity (number of documents) such that an instance is considered", "NUM2",
                            CommandLine.Type.INTEGER, true, false, true)
                    .withOption("o", "output", "the output file", "FILE", CommandLine.Type.FILE,
                            true, false, true)
                    .withOption("t", "timeout",
                            "the timeout for each query in seconds", "NUM3",
                            CommandLine.Type.INTEGER, true, false, true)
//                    .withFooter(
//                            "The SPARQL query for producing the taxonomy file is:\n\n"
//                                    + TAXONOMY_QUERY
//                                    + "\n\nThe SPARQL query for producing the instances file is:\n\n"
//                                    + INSTANCES_QUERY
//                                    + "\n\nNote: in the above query, replace %d with the number corresponding to the\n"
//                                    + "minimum number of linked documents for a type to be reported.")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

//            final File instancesFile = cmd.getOptionValue("i", File.class);
//            final File taxonomyFile = cmd.getOptionValue("t", File.class);
            final File outputFile = cmd.getOptionValue("o", File.class);
            //final int numDocuments = cmd.getOptionValue("d", Integer.class);
            final int granularity = cmd.getOptionValue("g", Integer.class);
            final String serverURL = cmd.getOptionValue("s", String.class);
            final String username = Strings.emptyToNull(cmd.getOptionValue("u", String.class));
            final String password = Strings.emptyToNull(cmd.getOptionValue("p", String.class));
            final int timeoutSec = cmd.getOptionValue("t", Integer.class);


            ///
            //one client and session for all 3 queries
            final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                    .maxConnections(4).validateServer(false).connectionTimeout(3*   timeoutSec * 1000).build();
            try {


                final Session session;
                if (username != null && password != null) {
                    session = ks.newSession(username, password);
                } else {
                    session = ks.newSession();
                }
                //download(session, outputFile, dumpResources, dumpMentions);
                //queryFolder.newDirectoryStream();
                final LinkingAnalyzer analyzer = new LinkingAnalyzer(session, granularity, timeoutSec);
                final String report = analyzer.analyze();
                Files.write(report, outputFile, Charsets.UTF_8);

                session.close();

            } finally {
                ks.close();
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static class Type implements Comparable<Type> {

        final URI uri;

        final Set<URI> parents;

        final Set<URI> children;

        final int entities;

        final int totalEntities;

        final int documents;

        Type(final URI uri, final int occurrences, final int totalEntities, final int documents) {
            this.uri = Preconditions.checkNotNull(uri);
            this.parents = Sets.newHashSet();
            this.children = Sets.newHashSet();
            this.entities = occurrences;
            this.totalEntities = totalEntities;
            this.documents = documents;
        }

        @Override
        public int compareTo(final Type other) {
            int result = other.documents - this.documents;
            if (result == 0) {
                result = this.uri.stringValue().compareTo(other.uri.stringValue());
            }
            return result;
        }

    }


}
