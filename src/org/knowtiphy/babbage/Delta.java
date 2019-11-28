package org.knowtiphy.babbage;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;

/**
 * @author graham
 */
public class Delta
{
	//	the added and deleted triples for the delta

	private Model toAdd = ModelFactory.createDefaultModel();
	private Model toDelete = ModelFactory.createDefaultModel();

	public Delta()
	{
	}

	//	needs to go
	public Delta(Model toAdd, Model toDelete)
	{
		this.toAdd = toAdd;
		this.toDelete = toDelete;
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

	public Model getToAdd()
	{
		return toAdd;
	}

	public Model getToDelete()
	{
		return toDelete;
	}

	public void addR(String subject, String predicate, String object)
	{
		toAdd.add(toAdd.createStatement(R(toAdd, subject), P(toAdd, predicate), R(toAdd, object)));
	}

	public <T> void addL(String subject, String predicate, T object)
	{
		toAdd.add(toAdd.createStatement(R(toAdd, subject), P(toAdd, predicate), L(toAdd, object)));
	}

	public void add(StmtIterator stmts)
	{
		stmts.forEachRemaining(toAdd::add);
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

	public void delete(StmtIterator stmts)
	{
		stmts.forEachRemaining(toDelete::add);
	}
}