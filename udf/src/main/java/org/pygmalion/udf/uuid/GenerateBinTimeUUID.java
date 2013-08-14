package org.pygmalion.udf.uuid;

import java.io.IOException;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Generates a time UUID. Either give it a time to use or
 * don't specify one and get a time UUID based on the current
 * time. UUID value is returned in binary form. 
 *
 */
public class GenerateBinTimeUUID extends EvalFunc<DataByteArray> {
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		UUID rval = null;
		
		if (!input.isNull(0) && input.get(0) instanceof Long) {
			Long time = (Long) input.get(0);
			rval = TimeUUIDUtils.getTimeUUID(time);
		} else {
			rval = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		}
		
		return new DataByteArray(TimeUUIDUtils.asByteArray(rval));
	}
}
