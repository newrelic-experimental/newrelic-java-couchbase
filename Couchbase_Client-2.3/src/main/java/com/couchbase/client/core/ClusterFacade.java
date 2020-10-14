package com.couchbase.client.core;

import rx.Observable;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave(type=MatchType.Interface)
public abstract class ClusterFacade {

	@Trace(dispatcher=true)
	public <R extends CouchbaseResponse> Observable<R> send(CouchbaseRequest paramCouchbaseRequest) {
		
		Observable<R> observable = Weaver.callOriginal();
		
		return observable;
		
	}
}
