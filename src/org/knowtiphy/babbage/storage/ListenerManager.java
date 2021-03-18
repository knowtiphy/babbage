package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

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

	public void notifyListeners(Model model)
	{
		if (!model.isEmpty())
		{
			for (IStorageListener listener : listeners)
			{
				try
				{
					listener.handleEvent(model);
				}
				catch (Exception ex)
				{
					ex.printStackTrace();
					logger.warning("Notifying change listener failed :: " + ex.getLocalizedMessage());
				}
			}
		}
	}
}