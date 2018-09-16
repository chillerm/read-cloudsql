package hillermann.carl.examples.beam.cloudsql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@Slf4j
public class Table2TablePipeline {

    public interface Table2TablePipelineOptions extends PipelineOptions {
        @Description("Database and table to extract. (Format: \"database_name.table_name\")")
        @Default.String("test.DUMMY")
        String getInputTable();

        void setInputTable(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    static void run(Table2TablePipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        PCollection<Dummy> dummies = p
                .apply("ReadLines", JdbcIO.<Dummy>read().withDataSourceConfiguration(getTestDataSource())
                        .withQuery("SELECT * from " + options.getInputTable())
                        .withRowMapper((JdbcIO.RowMapper<Dummy>) resultSet -> new Dummy(
                                resultSet.getLong("ID"),
                                resultSet.getLong("ITEM_ID"),
                                resultSet.getFloat("PRICE"),
                                resultSet.getString("LAST_UPDATED")))
                        .withCoder(AvroCoder.of(Dummy.class)));

        // Write to Database.
        dummies.apply("Write to DB", JdbcIO.<Dummy>write()
                .withStatement(Dummy.toMergeStatement())
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Dummy>() {
                    @Override
                    public void setParameters(Dummy element, PreparedStatement query) throws Exception {
                        try {
                            query.setLong(1, element.getId());
                            query.setLong(2, element.getItemId());
                            query.setFloat(3, element.getPrice());
                            query.setTimestamp(4, Timestamp.valueOf(element.getLastUpdated()));
                        } catch (SQLException e) {

                        }
                    }
                })
                .withDataSourceConfiguration(getTestDataSource())
        );

        // Write out the sql statements.  Typically we would not do this, we would rather write the object as the
        //  statement itself does not have business value.
        dummies.apply("toString", MapElements
                .into(TypeDescriptors.strings())
                .via(input -> Dummy.toMergeStatement()))

                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        Table2TablePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Table2TablePipelineOptions.class);

        run(options);
    }

    @DefaultCoder(AvroCoder.class)
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Dummy {
        private long id;
        private long itemId;
        private float price;
        private String lastUpdated;

        private static String toMergeStatement() {
            // The insert below is really a merge that is idempotent.
            return "INSERT INTO test.REMOTE_DUMMY\n" +
                    "    (ID, ITEM_ID, PRICE, LAST_UPDATED)\n" +
                    "VALUES\n" +
                    "       (?, ?, ?, ?)\n" +
                    "ON DUPLICATE KEY UPDATE\n" +
                    "    ITEM_ID = IF(LAST_UPDATED <= VALUES(LAST_UPDATED), VALUES(ITEM_ID), ITEM_ID)\n" +
                    "    , PRICE = IF(LAST_UPDATED <= VALUES(LAST_UPDATED), VALUES(PRICE), PRICE)\n" +
                    "    , LAST_UPDATED = IF(LAST_UPDATED <= VALUES(LAST_UPDATED), VALUES(LAST_UPDATED), LAST_UPDATED)\n" +
                    ";";
        }
    }

    private static JdbcIO.DataSourceConfiguration getTestDataSource() {
        return JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver"
                , "jdbc:mysql://localhost:3306/test")
                .withUsername("root")
                .withPassword("test");
    }
}
