package hillermann.carl.examples.beam.cloudsql;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class WriteToSql {
    public interface WriteToSqlOptions extends PipelineOptions {
        @Description("Database and table to to update. (Format: \"database_name.table_name\")")
        @Default.String("test.test")
        String getTargetTable();

        void setTargetTable(String value);
    }

    static void run(WriteToSqlOptions options) {
        Pipeline p = Pipeline.create(options);
        Timestamp now = Timestamp.from(Instant.now());

        String upsert = "INSERT INTO test.test " +
                "( column_1, column_2, last_updated ) " +
                "VALUES (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "column_2 = ?, " +
                "last_updated = ? ;";


        List<RecordWithIdAndTimestamp> inputRecords = Arrays.asList(
                new RecordWithIdAndTimestamp(4, "four", now),
                new RecordWithIdAndTimestamp(1, "updatedOne", now),
                new RecordWithIdAndTimestamp(3, "updatedThree", now)
        );

        p
                .apply("Create Update Records", Create.of(inputRecords).withCoder(AvroCoder.of(RecordWithIdAndTimestamp.class)))
                .apply("Update Sql: ", JdbcIO.<RecordWithIdAndTimestamp>write()
                        .withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver"
                                        , "jdbc:mysql://localhost:3306/test")
                                        .withUsername("test")
                                        .withPassword("test")
                        )
                        .withStatement(upsert)
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<RecordWithIdAndTimestamp>() {
                            @Override
                            public void setParameters(RecordWithIdAndTimestamp element, PreparedStatement preparedStatement) throws Exception {
                                preparedStatement.setInt(1, element.getColumn_1());
                                preparedStatement.setString(2, element.getColumn2());
                                preparedStatement.setTimestamp(3, element.getLast_updated());
                                preparedStatement.setString(4, element.getColumn2());
                                preparedStatement.setTimestamp(5, element.getLast_updated());
                            }
                        })
                );


        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        WriteToSqlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WriteToSqlOptions.class);

        run(options);
    }


    @DefaultCoder(AvroCoder.class)
    public static class RecordWithIdAndTimestamp implements Serializable {
        private int column_1;
        private String column2;
        private long last_updated;

        public RecordWithIdAndTimestamp() {
        }

        public RecordWithIdAndTimestamp(int column_1, String column2, Timestamp last_updated) {
            this.column_1 = column_1;
            this.column2 = column2;
            this.last_updated = last_updated.toInstant().toEpochMilli();
        }

        public int getColumn_1() {
            return column_1;
        }

        public void setColumn_1(int column_1) {
            this.column_1 = column_1;
        }

        public String getColumn2() {
            return column2;
        }

        public void setColumn2(String column2) {
            this.column2 = column2;
        }

        public Timestamp getLast_updated() {
            return Timestamp.from(Instant.ofEpochMilli(last_updated));
        }

        public void setLast_updated(Timestamp last_updated) {
            this.last_updated = last_updated.toInstant().toEpochMilli();
        }
    }
}
