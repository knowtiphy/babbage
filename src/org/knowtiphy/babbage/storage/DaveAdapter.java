package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.knowtiphy.utils.JenaUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;

import static org.knowtiphy.babbage.storage.CALDAV.DStore.P;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.R;

public abstract class DaveAdapter extends BaseAdapter
{
	public DaveAdapter(String type, Dataset messageDatabase,
					   OldListenerManager listenerManager, ListenerManager newListenerManager,
					   BlockingDeque<Runnable> notificationQ)
	{
		super(type, messageDatabase, listenerManager, newListenerManager, notificationQ);
	}

	public String getStoredTag(String query, String resType)
	{
		String tag;
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			tag = JenaUtils.single(resultSet, soln -> soln.get(resType).toString());
		} finally
		{
			messageDatabase.end();
		}

		return tag;
	}

	public Set<String> getStored(String query, String resType)
	{
		Set<String> stored = new HashSet<>(1000);
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			stored.addAll(JenaUtils.set(resultSet, soln -> soln.get(resType).asResource().toString()));
		} finally
		{
			messageDatabase.end();
		}

		return stored;
	}

	public <T> void updateTriple(Model messageDB, Delta delta, String resURI, String hasProp, T updated)
	{
		delta.delete(messageDB.listStatements(R(messageDB, resURI), P(messageDB, hasProp), (RDFNode) null));
		delta.addDP(resURI, hasProp, updated);
	}

}
