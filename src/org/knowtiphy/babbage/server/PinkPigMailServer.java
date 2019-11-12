package org.knowtiphy.babbage.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.knowtiphy.babbage.messages.*;
import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.StorageException;
import org.knowtiphy.babbage.storage.StorageFactory;
import org.knowtiphy.utils.OS;


import javax.mail.MessagingException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

//	a Pink Pig Mail server process

public class PinkPigMailServer
{
	@SuppressWarnings("HardcodedFileSeparator")
	private static final String MESSAGE_STORAGE = "org/knowtiphy/pinkpigmail/messages";
	private static final String ACCOUNTS_FILE = "accounts.ttl";

	private static final Map<String, Class<?>> classMap = new HashMap<>(100);

	static
	{
		for (Class<?> cls : List.of(CloseMessage.class, CopyMessage.class, DeleteMessage.class,
				MarkAsAnsweredMessage.class, MarkAsJunkMessage.class, MarkAsReadMessage.class,
				MoveToJunkMessage.class, SendMessage.class))
		{
			classMap.put(cls.getSimpleName(), cls);
		}
	}

	//	TODO -- chose a better list type here
	private static final Collection<DataOutputStream> oStreams = new CopyOnWriteArrayList<>();

	// Comes in as Jena RDF, Sends back JSON back to ClientStorageObject

	private static void notifyListeners(Model added, Model removed)
	{
		// Pumps the models out to the client sockets
		for (DataOutputStream client : oStreams)
		{
			try
			{
				ByteArrayOutputStream stream = new ByteArrayOutputStream();
				RDFDataMgr.write(stream, added, Lang.RDFJSON);
				byte[] toAdd = stream.toByteArray();

				// Not sure on the method, will need to test
				client.write(toAdd);

				stream.reset();
				RDFDataMgr.write(stream, removed, Lang.RDFJSON);
				byte[] toRemove = stream.toByteArray();

				// Not sure on the method, will need to test
				client.write(toRemove);

			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	private static void processMessage(IStorage storage, String json) throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();

		int index = json.indexOf(' ');

		String messageType = json.substring(0, index);
		Class<?> cls = classMap.get(messageType);

		if (null == cls)
		{
			System.err.println("Unknown message type: " + messageType);
			//	TODO -- throw an exception here
		}
		else
		{
			String messageContent = json.substring(index + 1);
			((IMessage) mapper.readValue(messageContent, cls)).perform(storage);
		}
	}

	public static void main(String[] args)
			throws IOException, InterruptedException, StorageException, MessagingException, NoSuchMethodException,
			InstantiationException, IllegalAccessException, InvocationTargetException
	{
		//	get local message storage
		Path dir = Paths.get(Objects.requireNonNull(OS.getAppDir(PinkPigMailServer.class)).toString(), MESSAGE_STORAGE);
		Files.createDirectories(dir);
		IStorage storage = StorageFactory.getLocal(dir, OS.getAppFile(ClientStorage.class, ACCOUNTS_FILE));

		System.out.println("PINK PIG MAIL SERVER");

		//	add a single listener on the storage that takes changes and sends them to connected clients
		storage.addListener(PinkPigMailServer::notifyListeners);

		//	establish a server socket and listen for connections to it
		try (ServerSocket clients = new ServerSocket(6789))
		{
			//noinspection InfiniteLoopStatement
			while (true)
			{
				//	wait for a client to connection and spin up a thread to handle that connection
				try (Socket clientSocket = clients.accept())
				{
					@SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
					DataInputStream clientInput = (new DataInputStream(clientSocket.getInputStream()));
					oStreams.add(new DataOutputStream(clientSocket.getOutputStream()));
					Thread clientThread = new Thread(() -> {
						while (true)
						{
							try
							{
								processMessage(storage, clientInput.readUTF());
							} catch (Exception e)
							{
								e.printStackTrace();
							}
						}
					});

					clientThread.start();

				} catch (IOException ex)
				{
					ex.printStackTrace();
				}

				// Need a poison pill eventually as end condition
			}
		}
	}
}