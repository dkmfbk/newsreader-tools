package eu.fbk.nwrtools;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Strings;
import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.nwrtools.util.CommandLine;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KSContentReport {

    private static Logger LOGGER = LoggerFactory.getLogger(KSContentReport.class);

    public static void main(final String[] args) throws Throwable {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("KSContentReport")
                    .withHeader("Compute KS Entity Layer Content Statistics " //
                            + "TBD")
                    .withOption("s", "server", "the URL of the KS instance", "URL",
                            CommandLine.Type.STRING, true, false, true)
                    .withOption("u", "username", "the KS username (if required)", "USER",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("p", "password", "the KS password (if required)", "PWD",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("q", "queries", "the queries directory (files should have .qry extension", "DIR_Q", CommandLine.Type.DIRECTORY,
                            true, false, true)
                    .withOption("o", "output", "the output directory", "DIR_O", CommandLine.Type.DIRECTORY,
                            true, false, true)
//                    .withFooter(
//                            "The RDF format and compression type is automatically detected based on the\n"
//                                    + "file extension (e.g., 'tql.gz' = gzipped TQL file)")
                    .withOption("t", "timeout",
                            "the timeout for each query in seconds", "NUM",
                            CommandLine.Type.INTEGER, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

            final String serverURL = cmd.getOptionValue("s", String.class);
            final String username = Strings.emptyToNull(cmd.getOptionValue("u", String.class));
            final String password = Strings.emptyToNull(cmd.getOptionValue("p", String.class));
//            final boolean dumpResources = cmd.hasOption("r");
//            final boolean dumpMentions = cmd.hasOption("m");
            final File queryFolder = cmd.getOptionValue("q", File.class);
            final File outputFolder = cmd.getOptionValue("o", File.class);
            final int timeoutSec = cmd.getOptionValue("t", Integer.class);
            if (queryFolder.exists() && queryFolder.isDirectory()) {




                //one client for all queries
                final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                        .maxConnections(2).validateServer(false).connectionTimeout(timeoutSec * 1000).build();
                try {


                    for (final File fileEntry : queryFolder.listFiles(new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.toLowerCase().endsWith(".qry");
                        }
                    })) {

                        //one session for each query
                        final Session session;
                        if (username != null && password != null) {
                            session = ks.newSession(username, password);
                        } else {
                            session = ks.newSession();
                        }
                        //download(session, outputFile, dumpResources, dumpMentions);
                        //queryFolder.newDirectoryStream();


                        execQuery(session, fileEntry, outputFolder, timeoutSec);

                        session.close();
                    }

                } finally {
                    ks.close();

                }
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static void execQuery(final Session session, final File queryfile, final File resultfile, final Integer timeoutSec) throws Throwable {


        String query= getQueryFromFile(queryfile);

        File outputFile = new File(resultfile.getAbsolutePath() + File.separator +queryfile.getName().replace(".qry", "") + "_results.csv");
//        System.out.println(outputFile.getName());
//        System.out.println(outputFile.getAbsoluteFile());
//        System.out.println(outputFile.getAbsolutePath());

        Long timeoutMs = timeoutSec * 1000L;
        System.out.println(timeoutMs);
        Stream<BindingSet> stream = session.sparql(query).timeout(timeoutMs).execTuples();

        CSVWriter writer = new CSVWriter(new FileWriter(outputFile));

        List<BindingSet> tuples = stream.toList();
        @SuppressWarnings("unchecked")
        List<String> variables = stream.getProperty("variables", List.class);
        stream.close();
        for (BindingSet tuple : tuples) {
            System.out.println(tuple.toString());
            //Set<String> bindingNames = tuple.getBindingNames();
            List<String> record=new ArrayList<String>();;
            for (String var : variables) {
                //System.out.println(tuple.getValue(var).toString());
                record.add(tuple.getValue(var).toString());
            }
            writer.writeNext(record.toArray(new String[record.size()]));

        }

        writer.flush();
        writer.close();

    }

    private static String getQueryFromFile(final File queryfile) throws IOException {


        final BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(queryfile), "UTF8"));
        String query = "";
        String str;

        while ((str = in.readLine()) != null) {
            if (query.isEmpty())
                query = str;
            else
                query = query + "\n" + str;
        }

        return query;
    }


}


