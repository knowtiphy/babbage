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

	public void notifyChangeListeners(Delta delta)
	{
		if (!delta.getToAdd().isEmpty() || !delta.getToDelete().isEmpty())
		{
			for (IStorageListener listener : listeners)
			{
				try
				{
					listener.delta(delta.getToAdd(), delta.getToDelete());
				} catch (RuntimeException ex)
				{
					logger.warning("Notifying change listener failed :: " + ex.getLocalizedMessage());
				}
			}
		}
	}

	//	got to go
	public void notifyChangeListeners(Model added, Model removed)
	{
		notifyChangeListeners(new Delta(added, removed));
	}

	//	got to go
	public void notifyChangeListeners(Model model)
	{
		notifyChangeListeners(model, ModelFactory.createDefaultModel());
	}
}