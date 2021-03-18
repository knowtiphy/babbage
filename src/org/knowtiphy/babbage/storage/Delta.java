package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.utils.JenaUtils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author graham
 */
public class Delta
{
	//	the added and deleted triples for the delta

	private final Model adds = ModelFactory.createDefaultModel();
	private final Model deletes = ModelFactory.createDefaultModel();

//	public Delta()
//	{
//	}

	public Model getAdds()
	{
		return adds;
	}

	public Model getDeletes()
	{
		return deletes;
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

	public <T> Delta addDPN(String s, String p, T o)
	{
		JenaUtils.addDPN(adds, s, p, o);
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

	public void merge(Delta delta)
	{
		getAdds().add(delta.getAdds());
		getDeletes().add(delta.getDeletes());
	}

	public Delta delete(StmtIterator stmts)
	{
		deletes.add(stmts);
		return this;
	}

	public String toString()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		JenaUtils.printModel(adds.listStatements(), "+", pw);
		JenaUtils.printModel(deletes.listStatements(), "-", pw);
		return sw.toString();
	}
}