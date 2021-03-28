package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.utils.JenaUtils;

import java.io.StringWriter;

/**
 * @author graham
 */
public class Delta
{
	//	the added and deleted triples for the delta

	private final Model adds = ModelFactory.createDefaultModel();
	private final Model deletes = ModelFactory.createDefaultModel();

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
}