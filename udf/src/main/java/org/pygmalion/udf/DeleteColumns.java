package org.pygmalion.udf;


import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * EvalFunc to take the given set of column names and convert them
 * into a CassandraBag to delete those columns.  Works similarly
 * to ToCassandraBag.
 *
 * The first value in the input bag *has* to be the key. For
 * the rest of the fields, this UDF will interrogate the values
 * that you have named the variables to be the column names.
 */
public class DeleteColumns extends EvalFunc<Tuple>  {

    private static String UDFCONTEXT_SCHEMA_KEY = "cassandra.input_field_schema";
    private static String DELIM = "[\\s,]+";

    public Tuple exec(Tuple input) throws IOException {
        Tuple row = TupleFactory.getInstance().newTuple(2);
        DataBag columns = BagFactory.getInstance().newDefaultBag();
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(DeleteColumns.class);
        String fieldString = property.getProperty(UDFCONTEXT_SCHEMA_KEY);
        String [] fieldnames = fieldString.split(DELIM);

        // IT IS ALWAYS ASSUMED THAT THE OBJECT AT INDEX 0 IS THE ROW KEY

        for (int i=1; i<input.size(); i++) {
            if (input.get(i) instanceof DataBag) {
                for (Tuple cassandraColumn : (DataBag) input.get(i)) {
                    String name = cassandraColumn.get(0).toString();
                    columns.add(getColumnDef(name, null));
                }
            } else {
                columns.add(getColumnDef(fieldnames[i], null));
            }
        }

        row.set(0, input.get(0));
        row.set(1, columns);
        return row;
    }

    private Tuple getColumnDef(String name, Object value) throws ExecException {
        Tuple column = TupleFactory.getInstance().newTuple(2);
        column.set(0, name);
        column.set(1, value);
        return column;
    }

    public Schema outputSchema(Schema input) {
        StringBuilder builder = new StringBuilder();
        List<Schema.FieldSchema> fields = input.getFields();
        for (int i=0; i<fields.size(); i++) {
            builder.append(fields.get(i).alias);
            if (i != fields.size()-1) {
                builder.append(DELIM);
            }
        }

        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(DeleteColumns.class);
        property.setProperty(UDFCONTEXT_SCHEMA_KEY, builder.toString());

        return super.outputSchema(input);
    }
}
