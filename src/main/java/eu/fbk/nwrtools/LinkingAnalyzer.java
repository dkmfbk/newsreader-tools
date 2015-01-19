package eu.fbk.nwrtools;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.nwrtools.util.CommandLine;

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
            "  HAVING (COUNT(DISTINCT ?d) >= %d)\n" + //
            "  ORDER BY DESC(?documents)";

    private final File occurrencesFile;

    private final File taxonomyFile;

    private final int totalDocuments;

    private final Map<URI, Type> types;

    public LinkingAnalyzer(final File occurrencesFile, final File taxonomyFile,
            final int totalDocuments) {
        this.occurrencesFile = Preconditions.checkNotNull(occurrencesFile);
        this.taxonomyFile = Preconditions.checkNotNull(taxonomyFile);
        this.totalDocuments = totalDocuments;
        this.types = Maps.newHashMap();
    }

    public String analyze() {
        try {
            collectTypeOccurrences();
            collectTypeTaxonomy();
            return formatReport();
        } catch (final Throwable ex) {
            throw Throwables.propagate(ex);
        }
    }

    private void collectTypeOccurrences() throws Throwable {
        for (final String line : Files.readLines(this.occurrencesFile, Charsets.UTF_8)) {
            final String cleanedLine = cleanLine(line);
            final String[] tokens = cleanedLine.split("\\s+");
            try {
                final URI uri = new URIImpl(tokens[0]);
                final int entities = Integer.parseInt(tokens[1]);
                final int totalEntities = Integer.parseInt(tokens[2]);
                final int documents = Integer.parseInt(tokens[3]);
                Type type = this.types.get(uri);
                if (type == null) {
                    type = new Type(uri, entities, totalEntities, documents);
                    this.types.put(uri, type);
                }
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring instances line (" + ex.getMessage() + "): " + line);
            }
        }
    }

    private Multimap<URI, URI> collectTypeTaxonomy() throws Throwable {
        final Multimap<URI, URI> result = HashMultimap.create();
        for (final String line : Files.readLines(this.taxonomyFile, Charsets.UTF_8)) {
            final String cleanedLine = cleanLine(line);
            final String[] tokens = cleanedLine.split("\\s+");
            try {
                final URI parent = new URIImpl(tokens[0]);
                final URI child = new URIImpl(tokens[1]);
                final Type parentType = this.types.get(parent);
                final Type childType = this.types.get(child);
                if (parentType != null && childType != null) {
                    parentType.children.add(child);
                    childType.parents.add(parent);
                }
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring taxonomy line (" + ex.getMessage() + "): " + line);
            }
        }
        return result;
    }

    private String cleanLine(final String line) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < line.length(); ++i) {
            final char c = line.charAt(i);
            if (c == ',') {
                builder.append(' ');
            } else if (c != '\'' && c != '"' && c != '<' && c != '>') {
                builder.append(c);
            }
        }
        return builder.toString();
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
                    .withOption("i", "instances", "the instancesfile (see below)", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("t", "taxonomy", "the taxonomy file (see below)", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("d", "documents",
                            "the total number of documents (for computing percentages)", "NUM",
                            CommandLine.Type.INTEGER, true, false, true)
                    .withOption("o", "output", "the output file", "FILE", CommandLine.Type.FILE,
                            true, false, true)
                    .withFooter(
                            "The SPARQL query for producing the taxonomy file is:\n\n"
                                    + TAXONOMY_QUERY
                                    + "\n\nThe SPARQL query for producing the instances file is:\n\n"
                                    + INSTANCES_QUERY
                                    + "\n\nNote: in the above query, replace %d with the number corresponding to the\n"
                                    + "minimum number of linked documents for a type to be reported.")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

            final File instancesFile = cmd.getOptionValue("i", File.class);
            final File taxonomyFile = cmd.getOptionValue("t", File.class);
            final File outputFile = cmd.getOptionValue("o", File.class);
            final int numDocuments = cmd.getOptionValue("d", Integer.class);

            final LinkingAnalyzer analyzer = new LinkingAnalyzer(instancesFile, taxonomyFile,
                    numDocuments);
            final String report = analyzer.analyze();
            Files.write(report, outputFile, Charsets.UTF_8);

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
