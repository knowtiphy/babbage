package org.knowtiphy.babbage.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class OldListenerManager
{
	private static final Logger logger = Logger.getLogger(OldListenerManager.class.getName());

	private final Collection<IOldStorageListener> listeners = new ArrayList<>(100);

	public void addListener(IOldStorageListener listener)
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
			for (IOldStorageListener listener : listeners)
			{
				try
				{
					listener.delta(delta.getAdds(), delta.getDeletes());
				}
				catch (Exception ex)
				{
					logger.warning("Old notifying change listener failed :: " + ex.getLocalizedMessage());
				}
			}
		}
	}
}