package org.knowtiphy.babbage.server;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.knowtiphy.babbage.storage.*;

import javax.mail.MessagingException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.StorageException;

// So this class will turn things into JSON, and it's Delta method will turn things back into JAVA
// could also have some start method here
//NOT REALLY SURE WHAT THIS CLASS IF FOR??????
public class ClientStorage implements IStorage
{
	private final PinkPigMailServer server;
	private IStorageListener listener;
	private final DataInputStream clientInput;
	private final DataOutputStream clientOutput;

	public ClientStorage() throws IOException
	{
		server = new PinkPigMailServer();
		//server.start();

		// Connect to org.knowtiphy.pinkpigmail.server
		Socket clientSocket = new Socket("10.100.41.82", 6789);
		clientInput = new DataInputStream(clientSocket.getInputStream());
		clientOutput = new DataOutputStream(clientSocket.getOutputStream());

		// Will write JSON to this socket, and will read back JSON, and then turn said JSON back into RDF which we did in the LocalStorage Layer
		// Will read JSON from the socket, and translate RDF back to the client

		// IStorageListener and then call the delta on it. so that when an update comes in from the org.knowtiphy.pinkpigmail.server, the client gets the update
		// Trigger delta in client when you get something from the socket
		Thread fromServer = new Thread(() -> {
			while (true)
			{
				try
				{
					Model added = ModelFactory.createDefaultModel();
					added.read(new StringReader(new String(clientInput.readAllBytes(), StandardCharsets.UTF_8)), null, "RDF/JSON");

					Model removed = ModelFactory.createDefaultModel();
					removed.read(new StringReader(new String(clientInput.readAllBytes(), StandardCharsets.UTF_8)), null, "RDF/JSON");

					listener.delta(added, removed);

				} catch (IOException e)
				{
					e.printStackTrace();
				}


			}
		});

		fromServer.start();
	}

	// These methods will send their info as JSON and write to the socket, also include header
	@Override
	public Map<String, FutureTask<?>> addListener(IStorageListener listener)
			throws StorageException, MessagingException, InterruptedException
	{
		this.listener = listener;
		assert this.listener != null;
		return null;//PinkPigMailServer.addListener(null);
	}

	@Override
	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId)
	{
		return null;
	}


	@Override
	public void send(MessageModel model) throws StorageException
	{

	}

	@Override
	public IReadContext getReadContext()
	{
		return null;
	}

	@Override
	public void close()
	{

	}

	@Override
	public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds)
	{
		return null;
	}

	@Override
	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
										Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return null;
	}

	@Override
	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
								  String targetFolderId, boolean delete)
	{
		return null;
	}

	@Override
	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
											boolean flag)
	{
		return null;
	}

	@Override
	public Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds,
										boolean flag)
	{
		return null;
	}

	@Override
	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
										boolean flag)
	{
		return null;
	}
}
