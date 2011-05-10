package org.pygmalion.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * UDF to take the data structure that Cassandra outputs:
 * <code>(key, columns: bag {T: tuple(name, value)})</code>
 * and projects out just the key and column values that I
 * would like to use:
 * <code>bag: {(key, columns: bag {values})}</code>
 *
 * We can not only specify column names but also prefixes.
 * In this case it will return the bag of name/value pairs
 * in the bag of values that is returned.
 *
 * Example:
 * ('account_id', 'tag*') as a set of fields to project
 * will return this bag of values:
 * (12345, {('tag123', 'lol'), ('tag456', 'cat')}
 *
 * We are returning a bag when we really want to
 * be returning a tuple. The reason is that if we
 * return a tuple then pig wraps it up in another
 * tuple (which we don't want). By stuffing our
 * single record result into a bag we can simply
 * FLATTEN it and get what we want.
 *
 * NB: When a row has a ton of columns and if we don't
 * specify a slice predicate, this will be inefficient.
 */
public class FromCassandraBag extends EvalFunc<Tuple> {

    private static String DELIM = "[\\s,]+";
    private static String GREEDY_OPERATOR = "*";

    public Tuple exec(Tuple input) throws IOException {
        // Size must be two (column_selector,cassandra_bag)
        if (input == null || input.size() < 2)
            throw new IOException("Invalid input. Please pass in both a list of column names and the columns themselves.");

        String columnSelector = input.get(0).toString();
        DataBag cassandraBag  = (DataBag)input.get(1);
        String[] selections   = columnSelector.split(DELIM);
        Tuple output          = TupleFactory.getInstance().newTuple(selections.length);

        for (int i = 0; i < selections.length; i++) {
            String selection = selections[i];
            if (selection.endsWith(GREEDY_OPERATOR)) {
                String namePrefix  = selection.substring(0,selection.length()-1);
                DataBag columnsBag = BagFactory.getInstance().newDefaultBag();

                // Find all columns in the input bag that begin with 'namePrefix'
                // and add them to the 'columnsBag'
                for (Tuple cassandraColumn : cassandraBag) {
                    String name = cassandraColumn.get(0).toString();
                    if (name.startsWith(namePrefix)) {
                        columnsBag.add(cassandraColumn);
                    }
                }

                // Sometimes this bag will have no columns in it, this _is_ the desired behavior.
                output.set(i, columnsBag);

            } else {

                // Find the column in the input bag that has a name equal to 'selection'
                // and add _only_ the value to the output tuple. This is what you actually
                // want since you're specifying both an order and a name in the 'columnSelector'
                // string.
                for (Tuple cassandraColumn : cassandraBag) {
                    String name = cassandraColumn.get(0).toString();
                    if (name.equals(selection)) {
                        output.set(i, cassandraColumn.get(1));
                        break;
                    }
                }
            }
        }
        return output;
    }

    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
