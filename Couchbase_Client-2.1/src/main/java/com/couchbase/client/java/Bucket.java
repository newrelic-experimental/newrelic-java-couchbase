package com.couchbase.client.java;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryPlan;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.expertservices.instrumentation.couchbase.CouchbaseQuery;
import com.nr.expertservices.instrumentation.couchbase.CouchbaseQueryConverter;

@Weave(type=MatchType.Interface)

public  abstract class Bucket {

	@Trace(leaf=true)
	public  abstract String name();

	@Trace(leaf=true)
	public  JsonDocument get(String id) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   JsonDocument get(String id, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D get(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D get(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D get(String id, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D get(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "get");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("get").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   List<JsonDocument> getFromReplica(String id, ReplicaMode type) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   List<JsonDocument> getFromReplica(String id, ReplicaMode type, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getFromReplica");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getFromReplica").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   JsonDocument getAndLock(String id, int lockTime) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   JsonDocument getAndLock(String id, int lockTime, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndLock(D document, int lockTime) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndLock(D document, int lockTime, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndLock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndLock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   JsonDocument getAndTouch(String id, int expiry) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   JsonDocument getAndTouch(String id, int expiry, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndTouch(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndTouch(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public   <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "getAndTouch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("getAndTouch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D insert(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D insert(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public   <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D insert(D document, PersistTo persistTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D insert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D insert(D document, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D insert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "insert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("insert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, PersistTo persistTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "upsert");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("upsert").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, PersistTo persistTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D replace(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "replace");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("replace").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, PersistTo persistTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, PersistTo persistTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, ReplicateTo replicateTo) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonDocument remove(String id, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "remove");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("remove").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  ViewResult query(ViewQuery viewQuery) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(viewQuery.getDesign()).operation("query").noInstance().noDatabaseName().build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  ViewResult query(ViewQuery viewQuery, long timeout, TimeUnit timeUnit) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(viewQuery.getDesign()).operation("query").noInstance().noDatabaseName().build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  QueryResult query(Query query) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(query.getClass().getSimpleName()).operation("query").noInstance().noDatabaseName().build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  QueryResult query(Query query, long timeout, TimeUnit timeUnit) {
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(query.getClass().getSimpleName()).operation("query").noInstance().noDatabaseName().build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  Boolean unlock(String id, long timeout) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "unlock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("unlock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  Boolean unlock(String id, long cas, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "unlock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("unlock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> Boolean unlock(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "unlock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("unlock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> Boolean unlock(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "unlock");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("unlock").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  Boolean touch(String id, int expiry) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "touch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("touch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  Boolean touch(String id, int expiry, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "touch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("touch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> Boolean touch(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "touch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("touch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> Boolean touch(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "touch");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("touch").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta, long initial, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta, long initial) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta, long initial, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta, long initial, int expiry) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  JsonLongDocument counter(String id, long delta, long initial, int expiry, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(id, name(), "counter");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("counter").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D append(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "append");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("prepend").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D append(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "append");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("prepend").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D prepend(D document) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "prepend");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("prepend").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  <D extends Document<?>> D prepend(D document, long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery(document, name(), "prepend");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("prepend").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public  Boolean close() {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery((String)null, name(), "close");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("close").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public  Boolean close(long timeout, TimeUnit timeUnit) {
		CouchbaseQuery couchbaseQ = new CouchbaseQuery((String)null, name(), "close");
    	DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(name()).operation("close").noInstance().noDatabaseName().slowQuery(couchbaseQ, new CouchbaseQueryConverter()).build();
    	NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}


	@Trace(leaf=true)
	public SpatialViewResult  query(SpatialViewQuery spatialViewQuery) {
		DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(spatialViewQuery.getDesign()).operation("query").noInstance().noDatabaseName().build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public SpatialViewResult query(SpatialViewQuery spatialViewQuery, long timeout, TimeUnit timeUnit) {
		DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(spatialViewQuery.getDesign()).operation("query").noInstance().noDatabaseName().build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public QueryResult query(Statement statement) {
		DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(statement.getClass().getSimpleName()).operation("query").noInstance().noDatabaseName().build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

	@Trace(leaf=true)
	public QueryResult query(Statement statement, long timeout, TimeUnit timeUnit) {
		DatastoreParameters params = DatastoreParameters.product("Couchbase").collection(statement.getClass().getSimpleName()).operation("query").noInstance().noDatabaseName().build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		return Weaver.callOriginal();
	}

}
