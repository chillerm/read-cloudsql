package hillermann.carl.examples.beam.cloudsql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.sql.ResultSet;


public class SqlToBigQuery {
    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    /**
     * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it
     * to a ParDo in the pipeline.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(WordCount.ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist = Metrics.distribution(
                WordCount.ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new WordCount.ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }

    /**
     * Options supported by {@link WordCount}.
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
     * to be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface SqlToBigQueryOptions extends PipelineOptions {
        @Description("Database and table to extract. (Format: \"database_name.table_name\")")
        @Default.String("test.test")
        String getInputTable();

        void setInputTable(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    static void run(SqlToBigQueryOptions options) {
        Pipeline p = Pipeline.create(options);

        p
                .apply("ReadLines", JdbcIO.<Dummy>read().withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver"
                                , "jdbc:mysql://localhost:3306/test")
                                .withUsername("test")
                                .withPassword("test")
                )
                        .withQuery("SELECT * from " + options.getInputTable())
                        .withRowMapper(new JdbcIO.RowMapper<Dummy>() {
                            @Override
                            public Dummy mapRow(ResultSet resultSet) throws Exception {
                                return new Dummy(resultSet.getInt("column_1")
                                        + resultSet.getString("column_2"));
                            }
                        })
                        .withCoder(AvroCoder.of(Dummy.class)))
                .apply("toString", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Dummy::getValue))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        SqlToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SqlToBigQueryOptions.class);

        run(options);
    }
}

class Dummy {
    String value;

    public Dummy(String value) {
        this.value = value;
    }

    public Dummy() {
        this.value = "default";
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Dummy{" +
                "value='" + value + '\'' +
                '}';
    }
}
