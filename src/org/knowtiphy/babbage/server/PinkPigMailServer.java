package org.knowtiphy.babbage.server;

//import com.fasterxml.jackson.databind.ObjectMapper;
import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.StorageFactory;

//	a Pink Pig Mail server process

public class PinkPigMailServer
{
	@SuppressWarnings("HardcodedFileSeparator")
	private static final String ACCOUNTS_FILE = "accounts.ttl";



	public static void main(String[] args) throws Exception
	{
		//	get local message storage
//		Path dir = Paths.get(Objects.requireNonNull(OS.getDataDir(PinkPigMailServer.class)).toString(), MESSAGE_STORAGE);
//		Files.createDirectories(dir);
		IStorage storage = StorageFactory.getLocal();//Paths.get(OS.getSettingsDir(ClientStorage.class).toString(), ACCOUNTS_FILE));

		System.out.println("PINK PIG MAIL SERVER");

		//	establish a server socket and listen for connections to it
//		try (ServerSocket clients = new ServerSocket(6789))
//		{
//			//noinspection InfiniteLoopStatement
//			while (true)
//			{
//				//	wait for a client to connection and spin up a thread to handle that connection
//				try (Socket clientSocket = clients.accept())
//				{
//					@SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
//					DataInputStream clientInput = (new DataInputStream(clientSocket.getInputStream()));
//					oStreams.add(new DataOutputStream(clientSocket.getOutputStream()));
//					Thread clientThread = new Thread(() -> {
//						while (true)
//						{
//							try
//							{
//								processMessage(storage, clientInput.readUTF());
//							}
//							catch (Exception e)
//							{
//								e.printStackTrace();
//							}
//						}
//					});
//
//					clientThread.start();
//
//				}
//				catch (IOException ex)
//				{
//					ex.printStackTrace();
//				}
//
//				// Need a poison pill eventually as end condition
//			}
//		}
	}

//	//	TODO -- chose a better list type here
//	private static final Collection<DataOutputStream> oStreams = new CopyOnWriteArrayList<>();
//
//	// Comes in as Jena RDF, Sends back JSON back to ClientStorageObject
//
//	private static void notifyListeners(Model added, Model removed)
//	{
//		// Pumps the models out to the client sockets
//		for (DataOutputStream client : oStreams)
//		{
//			try
//			{
//				ByteArrayOutputStream stream = new ByteArrayOutputStream();
//				RDFDataMgr.write(stream, added, Lang.RDFJSON);
//				byte[] toAdd = stream.toByteArray();
//
//				// Not sure on the method, will need to test
//				client.write(toAdd);
//
//				stream.reset();
//				RDFDataMgr.write(stream, removed, Lang.RDFJSON);
//				byte[] toRemove = stream.toByteArray();
//
//				// Not sure on the method, will need to test
//				client.write(toRemove);
//
//			}
//			catch (IOException e)
//			{
//				e.printStackTrace();
//			}
//		}
//	}

//	private static void processMessage(IStorage storage, String json) throws Exception
//	{
//		ObjectMapper mapper = new ObjectMapper();
//
//		int index = json.indexOf(' ');
//
//		String messageType = json.substring(0, index);
//		Class<?> cls = classMap.get(messageType);
//
//		if (null == cls)
//		{
//			System.err.println("Unknown message type: " + messageType);
//			//	TODO -- throw an exception here
//		}
//		else
//		{
//			String messageContent = json.substring(index + 1);
//			((IMessage) mapper.readValue(messageContent, cls)).perform(storage);
//		}
//	}
}