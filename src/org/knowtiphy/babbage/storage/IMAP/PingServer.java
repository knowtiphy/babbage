package org.knowtiphy.babbage.storage.IMAP;

import javax.mail.Folder;
import javax.mail.MessagingException;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class PingServer implements Runnable
{
	private static final Logger LOGGER = Logger.getLogger(PingServer.class.getName());

	private final IMAPAdapter adapter;
	private final Folder folder;
	private final long frequency;

	public PingServer(IMAPAdapter adapter, Folder folder, long frequency)
	{
		this.adapter = adapter;
		this.folder = folder;
		this.frequency = frequency;
	}

	@Override
	public void run()
	{
		while (true)
		{
			try
			{
				Thread.sleep(frequency);
				LOGGER.info("Pinging server");
				folder.getMessageCount();
			} catch (InterruptedException ex)
			{
				return;
			} catch (MessagingException ex)
			{
				LOGGER.warning(() -> "Pinging server failed :: " + ex.getLocalizedMessage());
				adapter.addWork(() ->
				{
					adapter.reStartPingThread();
					return null;
				});
			}
		}
	}
}