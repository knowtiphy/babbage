package org.knowtiphy.babbage.storage;

import java.io.IOException;

/**
 * @author graham
 */
public class StorageFactory
{
	public static IStorage getLocal() throws Exception
	{
		LocalStorageSandBox storage = new LocalStorageSandBox();
		return storage;
	}

	public static IStorage getRemote() throws IOException
	{
		return new RemoteStorage(6789);
	}
}