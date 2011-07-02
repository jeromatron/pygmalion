package org.pygmailion.udf;


import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.eclipse.jdt.internal.compiler.codegen.AttributeNamesConstants;
import org.junit.Test;
import org.pygmalion.udf.ToCassandraBag;

import java.util.Properties;

import static junit.framework.Assert.assertNotNull;

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
    }
}
