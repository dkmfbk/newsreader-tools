package eu.fbk.nwrtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Sets;

import eu.fbk.nwrtools.util.CommandLine;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Tracker;

public final class QueryTestGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTestGenerator.class);

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private final Dictionary dictionary;

    private final List<List<String>> fileVars;

    private final List<List<Tuple>> fileTuples;

    private final int[][] fileMappings; // m_ij is index in out vars of var j of file i

    private final List<String> outputVars;

    public QueryTestGenerator(final File... inputFiles) throws IOException {

        // Create a global dictionary for mapping values to codes and back
        this.dictionary = new Dictionary();

        // Read schema and tuples from input files
        this.fileVars = Lists.newArrayList();
        this.fileTuples = Lists.newArrayList();
        for (int i = 0; i < inputFiles.length; ++i) {
            final List<String> vars = Lists.newArrayList();
            final List<Tuple> tuples = Lists.newArrayList();
            read(inputFiles[i], vars, tuples, this.dictionary);
            this.fileVars.add(vars);
            this.fileTuples.add(tuples);
        }

        // Compute output schema and mappings from file to output schema
        this.fileMappings = new int[inputFiles.length][];
        this.outputVars = Lists.newArrayList();
        for (int i = 0; i < this.fileVars.size(); ++i) {
            boolean insidePrefix = true;
            this.fileMappings[i] = new int[this.fileVars.get(i).size()];
            for (int j = 0; j < this.fileMappings[i].length; ++j) {
                final String var = this.fileVars.get(i).get(j);
                int index = this.outputVars.indexOf(var);
                if (index < 0) {
                    insidePrefix = false;
                    index = this.outputVars.size();
                    this.outputVars.add(var);
                } else if (!insidePrefix) {
                    throw new IllegalArgumentException("Variable " + var + " of file "
                            + inputFiles[i].getAbsolutePath() + " matches var in previous files "
                            + "but is preceded by newly intruduced variable ");
                }
                this.fileMappings[i][j] = index;
            }
        }
        LOGGER.info("Output schema: ({})", Joiner.on(", ").join(this.outputVars));
    }

    public void generate(final int numCases, final File outputFile) throws IOException {

        // Use a tracker to display the progress of the operation
        final Tracker tracker = new Tracker(LOGGER, null, //
                "Generated %d tuples (%d tuple/s avg)", //
                "Generated %d tuples (%d tuple/s, %d tuple/s avg)");
        tracker.start();

        // Generate a set of (unique) joined tuples, of the size specified
        int numFailures = 0;
        int numDuplicates = 0;
        final Set<Tuple> outputTuples = Sets.newLinkedHashSet();
        final int[] outputCodes = new int[this.outputVars.size()];
        outer: while (outputTuples.size() < numCases) {
            Arrays.fill(outputCodes, 0);
            for (int i = 0; i < this.fileTuples.size(); ++i) {
                if (!pick(this.fileTuples.get(i), this.fileMappings[i], outputCodes)) {
                    ++numFailures;
                    // System.out.println(numFailures + ", " + i + ", "
                    // + Arrays.toString(outputCodes));
                    continue outer;
                }
            }
            if (outputTuples.add(Tuple.create(outputCodes))) {
                tracker.increment();
            } else {
                ++numDuplicates;
            }
        }

        // Signal completion
        tracker.end();

        // Log number of failures and number of duplicate tuples during generation
        LOGGER.info("Tuple generation statistics: {} attempts failed, {} duplicates", numFailures,
                numDuplicates);

        // Write resulting tuples
        write(outputFile, this.outputVars, outputTuples, this.dictionary);
    }

    private static boolean pick(final List<Tuple> tuples, final int[] mappings,
            final int[] outputCodes) {

        final int numVariables = mappings.length;
        final int numTuples = tuples.size();

        // The a-priori range where to pick the tuple is the full tuples list
        int start = 0;
        int end = tuples.size();

        // Check if range can be constrained based on codes previously assigned (i.e., join)
        boolean constrained = false;
        final int[] searchCodes = new int[numVariables];
        for (int i = 0; i < numVariables; ++i) {
            final int code = outputCodes[mappings[i]];
            if (code != 0) {
                searchCodes[i] = code;
                constrained = true;
            }
        }

        // If range can be constrained, build a 'search' tuple whose first codes are given by
        // variables previously assigned, and remaining variables are zero; then do binary search
        // followed by a scan for matching tuples to determine the range
        if (constrained) {
            final Tuple searchTuple = Tuple.create(searchCodes);
            start = Collections.binarySearch(tuples, searchTuple);
            if (start < 0) {
                start = -start - 1; // in case exact match not found
            }
            if (start >= numTuples || !tuples.get(start).matches(searchTuple)) {
                return false; // if range is empty or cannot join
            }
            end = start + 1;
            while (end < numTuples && tuples.get(end).matches(searchTuple)) {
                ++end;
            }
        }

        // Pick a random index inside the allowed range and use that tuple to augment output
        final int chosenIndex = start + RANDOM.nextInt(end - start);
        final Tuple chosenTuple = tuples.get(chosenIndex);
        for (int i = 0; i < numVariables; ++i) {
            final int slot = mappings[i];
            final int oldValue = outputCodes[slot];
            final int newValue = chosenTuple.get(i);
            if (oldValue != 0 && newValue != oldValue) {
                throw new Error("Join error: " + chosenTuple + " - "
                        + Arrays.toString(outputCodes) + " (search:  "
                        + Arrays.toString(searchCodes) + "; start " + start + "; end " + end + ")");
            }
            outputCodes[mappings[i]] = chosenTuple.get(i);
        }

        // Return true upon success
        return true;
    }

    private static void read(final File file, final List<String> vars, final List<Tuple> tuples,
            final Dictionary dictionary) throws IOException {

        // Read the file specified, populating the supplied vars and tuples list
        try (final BufferedReader reader = new BufferedReader(IO.utf8Reader(IO.buffer(IO.read(file
                .getAbsolutePath()))))) {

            // Read variables
            for (final String token : reader.readLine().split("\t")) {
                vars.add(token.trim().substring(1));
            }

            // Use a tracker to show the progress of the operation
            final Tracker tracker = new Tracker(LOGGER, null, //
                    "Parsed " + file.getAbsolutePath() + " (" + Joiner.on(", ").join(vars)
                            + "): %d tuples (%d tuple/s avg)", //
                    "Parsed %d tuples (%d tuple/s, %d tuple/s avg)");
            tracker.start();

            // Read data tuples, mapping values to codes using the dictionary
            int lineNum = 0;
            String line;
            final int[] codes = new int[vars.size()];
            while ((line = reader.readLine()) != null) {
                try {
                    ++lineNum;
                    final String[] tokens = line.split("\t");
                    for (int j = 0; j < codes.length; ++j) {
                        codes[j] = dictionary.codeFor(tokens[j]);
                    }
                    tuples.add(Tuple.create(codes));
                    tracker.increment();
                } catch (final Throwable ex) {
                    LOGGER.warn("Ignoring invalid line " + lineNum + " of file " + file + " - "
                            + ex.getMessage() + " [" + line + "]");
                }
            }

            // Signal completion
            tracker.end();

            // Sort read tuples
            Collections.sort(tuples);
        }

    }

    private static void write(final File file, final List<String> vars,
            final Collection<Tuple> tuples, final Dictionary dictionary) throws IOException {

        // Use a tracker to show the progress of the operation
        final Tracker tracker = new Tracker(LOGGER, null, //
                "Written " + file.getAbsolutePath() + " (" + Joiner.on(", ").join(vars)
                        + "): %d tuples (%d tuple/s avg)", //
                "Written %d tuples (%d tuple/s, %d tuple/s avg)");
        tracker.start();

        // Write to the file specified one line at a time
        final int numVars = vars.size();
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(file.getAbsolutePath())))) {

            // Start writing the header line: ?v1 ?v2 ...
            for (int i = 0; i < numVars; ++i) {
                if (i > 0) {
                    writer.write("\t");
                }
                writer.write("?");
                writer.write(vars.get(i));
            }
            writer.write("\n");

            // Write data lines
            for (final Tuple tuple : tuples) {
                for (int i = 0; i < numVars; ++i) {
                    if (i > 0) {
                        writer.write("\t");
                    }
                    writer.write(dictionary.stringFor(tuple.get(i)));
                }
                writer.write("\n");
                tracker.increment();
            }
        }

        // Signal completion
        tracker.end();
    }

    public static void main(final String[] args) {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("QueryTestGenerator")
                    .withHeader(
                            "Generates the test cases for the query test, "
                                    + "joining a number of input TSV files")
                    .withOption("c", "cases",
                            "the number of test cases to generate (default 1000)", "NUM",
                            CommandLine.Type.POSITIVE_INTEGER, true, false, false)
                    .withOption("i", "input", "the input TSV files", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, true, true)
                    .withOption("o", "output", "the output TSV file", "FILE",
                            CommandLine.Type.FILE, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

            final int numCases = cmd.getOptionValue("c", Integer.class, 1000);
            final List<File> inputFiles = cmd.getOptionValues("i", File.class);
            final File outputFile = cmd.getOptionValue("o", File.class);

            new QueryTestGenerator(inputFiles.toArray(new File[inputFiles.size()])).generate(
                    numCases, outputFile);

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static class Dictionary {

        private static final int TABLE_SIZE = 32 * 1024 * 1024 - 1;

        private static final int MAX_COLLISIONS = 1024;

        private static final int BUFFER_BITS = 12;

        private static final int BUFFER_SIZE = 1 << BUFFER_BITS;

        private final int[] table;

        private int[] list;

        private final List<byte[]> buffers;

        private int offset;

        private int lastCode;

        Dictionary() {
            this.table = new int[Dictionary.TABLE_SIZE];
            this.list = new int[1024];
            this.buffers = Lists.newArrayList();
            this.offset = BUFFER_SIZE;
            this.lastCode = 0;
        }

        public int codeFor(final String string) {
            final byte[] bytes = string.getBytes(Charsets.UTF_8);
            int bucket = Math.abs(string.hashCode()) % TABLE_SIZE;
            for (int i = 0; i < MAX_COLLISIONS; ++i) {
                final int code = this.table[bucket];
                if (code != 0) {
                    final int pointer = this.list[code - 1];
                    if (match(pointer, bytes)) {
                        return code;
                    }
                } else {
                    final int pointer = store(bytes);
                    if (this.lastCode >= this.list.length) {
                        final int[] oldList = this.list;
                        this.list = Arrays.copyOf(oldList, this.list.length * 2);
                    }
                    this.list[this.lastCode++] = pointer;
                    this.table[bucket] = this.lastCode;

                    // if (lastCode % 100000 == 0) {
                    // System.out.println(buffers.size() * BUFFER_SIZE);
                    // }

                    return this.lastCode;
                }
                bucket = (bucket + 1) % TABLE_SIZE;
            }
            throw new Error("Max number of collisions exceeded - RDF vocabulary too large");
        }

        public String stringFor(final int code) {
            final int pointer = this.list[code - 1];
            return new String(load(pointer), Charsets.UTF_8);
        }

        private byte[] load(final int pointer) {
            final int index = pointer >>> BUFFER_BITS - 2;
            final int offset = pointer << 2 & BUFFER_SIZE - 1;
            final byte[] buffer = this.buffers.get(index);
            int end = offset;
            while (buffer[end] != 0) {
                ++end;
            }
            return Arrays.copyOfRange(buffer, offset, end);
        }

        private int store(final byte[] bytes) {
            if (this.offset + bytes.length + 1 > BUFFER_SIZE) {
                this.buffers.add(new byte[BUFFER_SIZE]);
                this.offset = 0;
            }
            final int index = this.buffers.size() - 1;
            final int pointer = this.offset >> 2 | index << BUFFER_BITS - 2;
            final byte[] buffer = this.buffers.get(index);
            System.arraycopy(bytes, 0, buffer, this.offset, bytes.length);
            this.offset += bytes.length;
            buffer[this.offset++] = 0;
            this.offset = this.offset + 3 & 0xFFFFFFFC;
            return pointer;
        }

        private boolean match(final int pointer, final byte[] bytes) {
            final int index = pointer >>> BUFFER_BITS - 2;
            final int offset = pointer << 2 & BUFFER_SIZE - 1;
            final byte[] buffer = this.buffers.get(index);
            for (int i = 0; i < bytes.length; ++i) {
                if (buffer[offset + i] != bytes[i]) {
                    return false;
                }
            }
            return true;
        }

    }

    private static abstract class Tuple implements Comparable<Tuple> {

        public static Tuple create(final int... codes) {
            switch (codes.length) {
            case 0:
                return Tuple0.INSTANCE;
            case 1:
                return new Tuple1(codes[0]);
            case 2:
                return new Tuple2(codes[0], codes[1]);
            case 3:
                return new Tuple3(codes[0], codes[1], codes[2]);
            case 4:
                return new Tuple4(codes[0], codes[1], codes[2], codes[3]);
            default:
                return new TupleN(codes.clone());
            }
        }

        public abstract int size();

        public abstract int get(int index);

        public boolean matches(final Tuple tuple) {
            final int size = size();
            for (int i = 0; i < size; ++i) {
                final int expected = tuple.get(i);
                if (expected != 0 && get(i) != expected) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int compareTo(final Tuple other) {
            final int thisSize = size();
            final int otherSize = other.size();
            final int minSize = Math.min(thisSize, otherSize);
            for (int i = 0; i < minSize; ++i) {
                final int result = get(i) - other.get(i);
                if (result != 0) {
                    return result;
                }
            }
            return thisSize - otherSize;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Tuple)) {
                return false;
            }
            final Tuple other = (Tuple) object;
            final int size = size();
            if (other.size() != size) {
                return false;
            }
            for (int i = 0; i < size; ++i) {
                if (get(i) != other.get(i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            final int size = size();
            int hash = size;
            for (int i = 0; i < size; ++i) {
                hash = 37 * hash + get(i);
            }
            return hash;
        }

        @Override
        public String toString() {
            final int size = size();
            final StringBuilder builder = new StringBuilder();
            builder.append('(');
            for (int i = 0; i < size; ++i) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(get(i));
            }
            builder.append(')');
            return builder.toString();
        }

        private static final class Tuple0 extends Tuple {

            static final Tuple0 INSTANCE = new Tuple0();

            @Override
            public int size() {
                return 0;
            }

            @Override
            public int get(final int index) {
                throw new IndexOutOfBoundsException("Invalid index " + index);
            }

        }

        private static final class Tuple1 extends Tuple {

            private final int code;

            Tuple1(final int code) {
                this.code = code;
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public int get(final int index) {
                Preconditions.checkElementIndex(index, 1);
                return this.code;
            }

        }

        private static final class Tuple2 extends Tuple {

            private final int code0;

            private final int code1;

            Tuple2(final int code0, final int code1) {
                this.code0 = code0;
                this.code1 = code1;
            }

            @Override
            public int size() {
                return 2;
            }

            @Override
            public int get(final int index) {
                Preconditions.checkElementIndex(index, 2);
                return index == 0 ? this.code0 : this.code1;
            }

        }

        private static final class Tuple3 extends Tuple {

            private final int code0;

            private final int code1;

            private final int code2;

            Tuple3(final int code0, final int code1, final int code2) {
                this.code0 = code0;
                this.code1 = code1;
                this.code2 = code2;
            }

            @Override
            public int size() {
                return 3;
            }

            @Override
            public int get(final int index) {
                switch (index) {
                case 0:
                    return this.code0;
                case 1:
                    return this.code1;
                case 2:
                    return this.code2;
                default:
                    throw new IndexOutOfBoundsException("Index " + index + ", size 3");
                }
            }

        }

        private static final class Tuple4 extends Tuple {

            private final int code0;

            private final int code1;

            private final int code2;

            private final int code3;

            Tuple4(final int code0, final int code1, final int code2, final int code3) {
                this.code0 = code0;
                this.code1 = code1;
                this.code2 = code2;
                this.code3 = code3;
            }

            @Override
            public int size() {
                return 4;
            }

            @Override
            public int get(final int index) {
                switch (index) {
                case 0:
                    return this.code0;
                case 1:
                    return this.code1;
                case 2:
                    return this.code2;
                case 3:
                    return this.code3;
                default:
                    throw new IndexOutOfBoundsException("Index " + index + ", size 4");
                }
            }

        }

        private static final class TupleN extends Tuple {

            private final int[] codes;

            TupleN(final int[] codes) {
                this.codes = codes;
            }

            @Override
            public int size() {
                return this.codes.length;
            }

            @Override
            public int get(final int index) {
                return this.codes[index];
            }

        }

    }

}
