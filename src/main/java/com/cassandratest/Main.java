package com.cassandratest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.QueryValidationException;

public class Main {

	public static void main(String[] args) {

		PoolingOptions pools = new PoolingOptions();
		pools.setMaxSimultaneousRequestsPerConnectionThreshold(
				HostDistance.LOCAL, 50);
		pools.setCoreConnectionsPerHost(HostDistance.LOCAL, 128);
		pools.setMaxConnectionsPerHost(HostDistance.LOCAL, 128);
		pools.setCoreConnectionsPerHost(HostDistance.REMOTE, 128);
		pools.setMaxConnectionsPerHost(HostDistance.REMOTE, 128);

		Cluster cluster = new Cluster.Builder().addContactPoints("localhost")
				.withPoolingOptions(pools)
				.withSocketOptions(new SocketOptions().setTcpNoDelay(true))
				.build();
		
		Session session = cluster.connect();

        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
        
        try {
            session.execute("DROP KEYSPACE test");
        } catch (QueryValidationException e) { /* Fine, ignore */ }

        session.execute("CREATE KEYSPACE test WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        session.execute("USE test");
        
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE users_data (userid text, col text, val text, PRIMARY KEY (userid, col)) WITH COMPACT STORAGE");
        session.execute(sb.toString());
        
        sb = new StringBuilder();
        sb.append("INSERT INTO users_data (userid, col, val) VALUES('000000001', 'name', 'alex rewa')");
        session.execute(sb.toString());
        
        sb = new StringBuilder();
        sb.append("INSERT INTO users_data (userid, col, val) VALUES('000000001', 'email', 'a@a.com')");
        session.execute(sb.toString());
        
        sb = new StringBuilder();
        sb.append("INSERT INTO users_data (userid, col, val) VALUES('000000002', 'col', 'test')");
        session.execute(sb.toString());
        
	}

}
