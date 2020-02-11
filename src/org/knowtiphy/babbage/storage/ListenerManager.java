package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

	public void notifyChangeListeners(Collection<Delta> deltas)
	{
		//	TODO -- this is a little inefficient
		Delta delta = new Delta();
		deltas.forEach(delta::merge);

		if (!delta.getAdds().isEmpty() || !delta.getDeletes().isEmpty())
		{
			for (IStorageListener listener : listeners)
			{
				try
				{
					listener.delta(delta.getAdds(), delta.getDeletes());
				}
				catch (Exception ex)
				{
					logger.warning("Notifying change listener failed :: " + ex.getLocalizedMessage());
				}
			}
		}
	}

	//	got to go
	public void notifyChangeListeners(Model added, Model removed)
	{
		notifyChangeListeners(List.of(new Delta(added, removed)));
	}
}