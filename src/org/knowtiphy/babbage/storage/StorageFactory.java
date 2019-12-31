package org.knowtiphy.babbage.storage;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author graham
 */
public class StorageFactory
{
	public static IStorage getLocal() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
	{
		LocalStorageSandBox storage = new LocalStorageSandBox();
		return storage;
	}

	public static IStorage getRemote() throws IOException
	{
		return new RemoteStorage(6789);
	}
}