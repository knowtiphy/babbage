package org.knowtiphy.babbage.storage;

import javax.mail.MessagingException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

/**
 * @author graham
 */
public class StorageFactory
{
	public static IStorage getLocal(Path dir, Path accountsFile)
			throws InterruptedException, MessagingException, IOException, StorageException, InvocationTargetException,
			NoSuchMethodException, InstantiationException, IllegalAccessException
	{
		LocalStorageSandBox storage = new LocalStorageSandBox(dir, accountsFile);
		return storage;
	}

	public static IStorage getRemote() throws IOException
	{
		return new RemoteStorage(6789);
	}
}