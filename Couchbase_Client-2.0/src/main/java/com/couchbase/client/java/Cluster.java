package com.couchbase.client.java;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.transcoder.Transcoder;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave(type=MatchType.Interface)
public abstract class Cluster {

	@Trace(leaf=true)
	public Bucket openBucket() {
		Bucket bucket = Weaver.callOriginal();
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(bucket.name()).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return bucket;
	}
	
	@Trace(leaf=true)
    public Bucket openBucket(long timeout, TimeUnit timeUnit) {
		Bucket bucket = Weaver.callOriginal();
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(bucket.name()).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return bucket;
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name, long timeout, TimeUnit timeUnit) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name, String password) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name, String password, long timeout, TimeUnit timeUnit) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

	@Trace(leaf=true)
    public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders, long timeout, TimeUnit timeUnit) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name).operation("openBucket").build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
    	return Weaver.callOriginal();
    }

}
