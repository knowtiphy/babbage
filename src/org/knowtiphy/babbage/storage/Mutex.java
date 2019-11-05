package org.knowtiphy.babbage.storage;

public class Mutex
{
	/**
	 * The lock status
	 **/
	private boolean inuse_ = false;

	public void lock() throws InterruptedException
	{
		if (Thread.interrupted())
		{
			throw new InterruptedException();
		}
		synchronized (this)
		{
			try
			{
				while (inuse_)
				{
					wait();
				}
				inuse_ = true;
			} catch (InterruptedException ex)
			{
				notify();
				throw ex;
			}
		}
	}

	public synchronized void unlock()
	{
		inuse_ = false;
		notify();
	}
}