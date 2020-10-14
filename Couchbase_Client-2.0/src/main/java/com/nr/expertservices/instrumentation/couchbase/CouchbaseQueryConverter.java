package com.nr.expertservices.instrumentation.couchbase;

import java.util.logging.Level;

import com.newrelic.api.agent.Logger;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.QueryConverter;

public class CouchbaseQueryConverter implements QueryConverter<CouchbaseQuery> {

	@Override
	public String toRawQueryString(CouchbaseQuery rawQuery) {
		Logger logger = NewRelic.getAgent().getLogger();
		logger.log(Level.FINE, "enter toRqwQuery({0})", rawQuery);
		StringBuffer sb = new StringBuffer();
		sb.append(rawQuery.getOperation());
		sb.append(' ');
		sb.append(rawQuery.getOperation());
		if(rawQuery.getId() != null) {
			sb.append(" id: ");
			sb.append(rawQuery.getId());
		} else if(rawQuery.getDocument() != null) {
			sb.append(" document: ");
			sb.append(rawQuery.getDocument().id());
		}
		logger.log(Level.FINE, "returning {0}", sb.toString());
		return sb.toString();
	}

	@Override
	public String toObfuscatedQueryString(CouchbaseQuery rawQuery) {
		Logger logger = NewRelic.getAgent().getLogger();
		logger.log(Level.FINE, "enter toObfuscatedQueryString({0})", rawQuery);
		StringBuffer sb = new StringBuffer();
		sb.append(rawQuery.getOperation());
		sb.append(' ');
		sb.append(rawQuery.getOperation());
		sb.append(' ');
		if(rawQuery.getId() != null) {
			sb.append(" id: ?");
		} else if(rawQuery.getDocument() != null) {
			sb.append(" document: ?");
		}
		logger.log(Level.FINE, "returning {0}", sb.toString());
		return sb.toString();
	}

}
