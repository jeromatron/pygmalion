package org.pygmailion.udf;


import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import org.pygmalion.udf.RangeBasedStringConcat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class RangeBasedStringConcatTest {
    private String[] fields = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};

    @Test
    public void testAllConcat() throws Exception {
        RangeBasedStringConcat rbsc = new RangeBasedStringConcat("ALL", " ");
        Tuple input = new DefaultTuple();
        for (int i = 0; i < fields.length; i++) {
            input.append(fields[i]);
        }
        String result = rbsc.exec(input);
        assertEquals("a b c d e f g h i", result);
        Tuple innerTuple = new DefaultTuple();
        innerTuple.append("j");
        innerTuple.append("k");

        input.append(innerTuple);
        result = rbsc.exec(input);
        assertEquals("a b c d e f g h i j k", result);
        DataBag db = new DefaultDataBag();
        Tuple dbTuple = new DefaultTuple();
        dbTuple.append("l");
        dbTuple.append("m");
        db.add(dbTuple);
        innerTuple.append(db);
        result = rbsc.exec(input);
        assertEquals("a b c d e f g h i j k l m", result);
    }

    @Test
    public void testRange() throws Exception {
        RangeBasedStringConcat rbsc = new RangeBasedStringConcat("0,1", " ");
        Tuple input = new DefaultTuple();
        for (String field : fields) {
            input.append(field);
        }
        String result = rbsc.exec(input);
        assertEquals("a b", result);
        rbsc = new RangeBasedStringConcat("2,6", " ");
        result = rbsc.exec(input);
        assertEquals("c g", result);
        //test out of range
        rbsc = new RangeBasedStringConcat("0,9,1000", " ");
        result = rbsc.exec(input);
        assertEquals("a", result);

        Tuple innerTuple = new DefaultTuple();
        innerTuple.append("j");
        innerTuple.append("k");

        input.append(innerTuple);
        rbsc = new RangeBasedStringConcat("0,9", " ");
        result = rbsc.exec(input);
        assertEquals("a j k", result);
        DataBag db = new DefaultDataBag();
        Tuple dbTuple = new DefaultTuple();
        dbTuple.append("l");
        dbTuple.append("m");
        db.add(dbTuple);
        innerTuple.append(db);
        rbsc = new RangeBasedStringConcat("0,9,10", " ");
        result = rbsc.exec(input);
        assertEquals("a j k l m", result);
    }
}
