package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
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

	private Model adds = ModelFactory.createDefaultModel();
	private Model deletes = ModelFactory.createDefaultModel();

	public Delta()
	{
	}

	//	needs to go
	public Delta(Model adds, Model deletes)
	{
		this.adds = adds;
		this.deletes = deletes;
	}

	//	helper methods

	private static Resource R(Model model, String uri)
	{
		return model.createResource(uri);
	}

	private static Property P(Model model, String uri)
	{
		return model.createProperty(uri);
	}

	private static <T> Literal L(Model model, T value)
	{
		assert !(value instanceof Literal);
		return model.createTypedLiteral(value);
	}

	public Model getAdds()
	{
		return adds;
	}

	public Model getDeletes()
	{
		return deletes;
	}

	public Delta addR(String subject, String predicate, String object)
	{
		adds.add(adds.createStatement(R(adds, subject), P(adds, predicate), R(adds, object)));
		return this;
	}

	public <T> Delta addL(String subject, String predicate, T object)
	{
		adds.add(adds.createStatement(R(adds, subject), P(adds, predicate), L(adds, object)));
		return this;
	}

	public Delta add(StmtIterator stmts)
	{
		stmts.forEachRemaining(adds::add);
		return this;

	}

//	public <T> void deleteR(String subject, String predicate, String object)
//	{
//		toDelete.add(toDelete.createStatement(R(toDelete, subject), P(toDelete, predicate), R(toDelete, object)));
//	}
//
//	public <T> void deleteL(String subject, String predicate, T object)
//	{
//		toDelete.add(toDelete.createStatement(R(toDelete, subject), P(toDelete, predicate), L(toDelete, object)));
//	}

	public Delta delete(StmtIterator stmts)
	{
		stmts.forEachRemaining(deletes::add);
		return this;
	}

	public String toString()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		StringBuilder builder = new StringBuilder();
		JenaUtils.printModel(adds.listStatements(), "+", pw);
		JenaUtils.printModel(deletes.listStatements(), "-", pw);
		return builder.toString();
	}
}