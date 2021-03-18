package org.knowtiphy.babbage.storage;

/**
 * @author graham
 */
public class StorageFactory
{
	public static IStorage getLocal() throws Exception
	{
		return new LocalStorageSandBox();
	}

//	public static IStorage getRemote() throws IOException
//	{
//		return new RemoteStorage(6789);
//	}
}