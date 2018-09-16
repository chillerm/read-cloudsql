package hillermann.carl.examples.beam.cloudsql;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

class PlayGroundUtils {
    static JdbcIO.DataSourceConfiguration getTestDataSource() {
        return JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver"
                , "jdbc:mysql://localhost:3306/test")
                .withUsername("root")
                .withPassword("test");
    }
}
