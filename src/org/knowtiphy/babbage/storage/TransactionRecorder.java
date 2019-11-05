package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.listeners.NullListener;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author graham
 */
public class TransactionRecorder extends NullListener
{
	private final List<Statement> added = new LinkedList<>();
	private final List<Statement> removed = new LinkedList<>();

	public List<Statement> getAdded()
	{
		return added;
	}

	public List<Statement> getRemoved()
	{
		return removed;
	}

	public void abort()
	{
		added.clear();
		removed.clear();
	}

	@Override
	public void addedStatement(Statement statement)
	{
		added.add(statement);
	}

	@Override
	public void addedStatements(Statement[] statements)
	{
		added.addAll(Arrays.asList(statements));
	}

	@Override
	public void addedStatements(List<Statement> statements)
	{
		added.addAll(statements);
	}

	@Override
	public void addedStatements(StmtIterator statements)
	{
		added.addAll(statements.toList());
	}

	@Override
	public void addedStatements(Model model)
	{
		added.addAll(model.listStatements().toList());
	}

	@Override
	public void removedStatement(Statement statement)
	{
		removed.add(statement);
	}

	@Override
	public void removedStatements(Statement[] statements)
	{
		removed.removeAll(Arrays.asList(statements));
	}

	@Override
	public void removedStatements(List<Statement> statements)
	{
		removed.addAll(statements);
	}

	@Override
	public void removedStatements(Model model)
	{
		removed.addAll(model.listStatements().toList());
	}

	@Override
	public void removedStatements(StmtIterator statements)
	{
		removed.addAll(statements.toList());
	}
}