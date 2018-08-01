package hillermann.carl.examples.beam.cloudsql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.sql.ResultSet;


public class SqlToText {

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
                                return new Dummy((resultSet.getInt("column_1")
                                        + resultSet.getString("column_2")
                                        + resultSet.getTimestamp("last_updated")));
                            }
                        })
                        .withCoder(AvroCoder.of(Dummy.class)))
                .apply("toString", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Dummy::getValue))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        SqlToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SqlToBigQueryOptions.class);

        run(options);
    }

    private static class Dummy {
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

}
