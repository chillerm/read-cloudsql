package hillermann.carl.examples.beam.cloudsql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dummy {
    private long id;
    private long itemId;
    private float price;
    private String lastUpdated;

    public static String toMergeStatement() {
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