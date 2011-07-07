package org.pygmailion.udf;


import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.eclipse.jdt.internal.compiler.codegen.AttributeNamesConstants;
import org.junit.Test;
import org.pygmalion.udf.ToCassandraBag;

import java.io.IOException;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class ToCassandraBagTest {
    private String [] fields = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};

    @Test
    public void test() throws Exception {
        ToCassandraBag tcb = new ToCassandraBag();
        UDFContext context = UDFContext.getUDFContext();
        Properties properties = context.getUDFProperties(ToCassandraBag.class);
        Tuple input = new DefaultTuple();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.length; i++){
            builder.append(fields[i]);
            input.append("foo" + i);
            if (i < fields.length - 1){
                builder.append(',');
            }
        }
        properties.setProperty(ToCassandraBag.UDFCONTEXT_SCHEMA_KEY, builder.toString());
        Tuple tuple = tcb.exec(input);
        assertNotNull("Tuple is null", tuple);
        assertEquals(2, tuple.size());
        //first is the key, rest is a set of columns
        Object one = tuple.get(0);
        assertTrue(one instanceof String);
        Object two = tuple.get(1);
        assertTrue(two instanceof DataBag);
        //Bad input
        input = new DefaultTuple();
        input.append(null);
        input.append("foo");
        try {
            tcb.exec(input);
            assertTrue(false);
        } catch (IOException e) {
            //expected
        }
        input = new DefaultTuple();
        builder.setLength(0);
        for (int i = 0; i < fields.length -1; i++){
            builder.append(fields[i]);
            input.append("foo" + i);
            if (i < fields.length - 1){
                builder.append(',');
            }
        }
        properties.setProperty(ToCassandraBag.UDFCONTEXT_SCHEMA_KEY, builder.toString());
        input.append("foo extra");
        try {
            tcb.exec(input);
            assertTrue(false);
        } catch (IOException e) {

        }
    }
}
