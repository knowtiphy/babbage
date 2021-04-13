package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.LoggerUtils;

import java.io.StringWriter;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class Delta
{
	private static final Logger LOGGER = Logger.getLogger(Delta.class.getName());

	private final Dataset dataSet;

	//	the added and deleted triples for the delta

	private final Model adds = ModelFactory.createDefaultModel();
	private final Model deletes = ModelFactory.createDefaultModel();

	public Delta(Dataset dataSet)
	{
		this.dataSet = dataSet;
	}

	public Model getAdds()
	{
		return adds;
	}

	public Model getDeletes()
	{
		return deletes;
	}

	public Delta addType(String s, String type)
	{
		JenaUtils.addType(adds, s, type);
		return this;
	}

	public Delta addOP(String s, String p, String o)
	{
		JenaUtils.addOP(adds, s, p, o);
		return this;
	}

	public <T> Delta addDP(String s, String p, T o)
	{
		JenaUtils.addDP(adds, s, p, o);
		return this;
	}

	public Delta add(StmtIterator stmts)
	{
		adds.add(stmts);
		return this;
	}

	public Delta add(Model model)
	{
		adds.add(model);
		return this;
	}

	public Delta delete(StmtIterator stmts)
	{
		deletes.add(stmts);
		return this;
	}

	public Delta deleteOP(String s, String p, String o)
	{
		JenaUtils.addOP(deletes, s, p, o);
		return this;
	}

	//	TODO -- check this is right (trying to ensure they are only in the dbase once)
	public Delta bothOP(String s, String p, String o)
	{
		JenaUtils.addOP(deletes, s, p, o);
		JenaUtils.addOP(adds, s, p, o);
		return this;
	}

	//	TODO -- check this is right (trying to ensure they are only in the dbase once)
	public Delta bothDP(String s, String p, String o)
	{
		JenaUtils.addDP(deletes, s, p, o);
		JenaUtils.addDP(adds, s, p, o);
		return this;
	}

	public Delta bothDPN(String s, String p, String o)
	{
		JenaUtils.addDPN(deletes, s, p, o);
		JenaUtils.addDPN(adds, s, p, o);
		return this;
	}

	public String toString()
	{
		StringWriter sw = new StringWriter();
		JenaUtils.printModel(adds, "+", sw);
		JenaUtils.printModel(deletes, "-", sw);
		return sw.toString();
	}

	public static Delta merge(Dataset dSet, Collection<Delta> deltas)
	{
		var merged = new Delta(dSet);
		deltas.forEach(d -> {
			merged.adds.add(d.adds);
			merged.deletes.add(d.deletes);
		});

		return merged;
	}

	//	apply the change represented by the delta

	public void apply()
	{
		dataSet.begin(ReadWrite.WRITE);
		try
		{
			//	do deletes before adds in case adds are replacing things that are being deleted
			dataSet.getDefaultModel().remove(getDeletes());
			dataSet.getDefaultModel().add(getAdds());
			dataSet.commit();
		}
		catch (Exception ex)
		{
			//	if this happens were are in deep shit with no real way of recovering
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			dataSet.abort();
		}
		finally
		{
			dataSet.end();
		}
	}
}