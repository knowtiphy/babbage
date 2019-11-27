package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class ListenerManager
{
	private static final Logger logger = Logger.getLogger(ListenerManager.class.getName());

	private final Collection<IStorageListener> listeners = new ArrayList<>(100);

	public void addListener(IStorageListener listener)
	{
		listeners.add(listener);
	}

	public void notifyChangeListeners(Model added, Model removed)
	{
		if (!added.isEmpty() || !removed.isEmpty())
		{
			for (IStorageListener listener : listeners)
			{
				try
				{
					listener.delta(added, removed);
				} catch (RuntimeException ex)
				{
					logger.warning("Notifying change listener failed :: " + ex.getLocalizedMessage());
				}
			}
		}
	}

	public void notifyChangeListeners(TransactionRecorder recorder)
	{
		Model added = ModelFactory.createDefaultModel();
		added.add(recorder.getAdded());
		Model removed = ModelFactory.createDefaultModel();
		removed.add(recorder.getRemoved());
		notifyChangeListeners(added, removed);
	}
}