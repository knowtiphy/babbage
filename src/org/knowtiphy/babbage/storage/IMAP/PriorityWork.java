package org.knowtiphy.babbage.storage.IMAP;

import org.knowtiphy.utils.HasPriority;

import java.util.concurrent.Callable;

class PriorityWork<T> implements Callable<T>, HasPriority
{
	private Callable<T> operation;
	private int priority;

	public PriorityWork(Callable<T> operation, int priority)
	{
		this.operation = operation;
		this.priority = priority;
	}

	@Override
	public T call() throws Exception
	{
		return operation.call();
	}

	@Override
	public int getPriority()
	{
		return priority;
	}
}