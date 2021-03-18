package org.knowtiphy.babbage.storage;

//	a class to use on the client to talk to a remote storage layer (so a layer on the same machine, but not in
//	the same process)
public class RemoteStorage //implements IStorage
{
//	private final Socket socket;
//	private final OutputStream output;
//
//	public RemoteStorage(int port) throws IOException
//	{
//		socket = new Socket("localhost", port);
//		output = new DataOutputStream(socket.getOutputStream());
//	}
//	@Override
//	public ResultSet query(String id, String query)
//	{
//		return null;
//	}
//
//	//	send a message to a remote server
//	private Future<?> send(IMessage message)
//	{
//		Future<?> task = null; //TODO -- new FutureTask<Void>();
//		ObjectMapper objectMapper = new ObjectMapper();
//		//	add the header
//		//	TODO -- is this correct?
//		try
//		{
//			output.write(message.getClass().getSimpleName().getBytes());
//			output.write(" ".getBytes());
//			objectMapper.writeValue(output, message);
//		} catch (IOException ex)
//		{
//			//	need to set the exception for the task
//		}
//		//	TODO -- futures?
//		return task;
//	}
//	@Override
//	public Model getAccounts()
//	{
//		return null;
//	}
//
//	@Override
//	public Map<String, FutureTask<?>> addListener(IStorageListener listener)
//			throws StorageException, MessagingException, InterruptedException
//	{
//		//	TODO
//		return null;
//	}
//
//	public Future<?> send(Model model) throws StorageException
//	{
//		return null;
//	}
//
//	@Override
//	public void send(MessageModel model) throws StorageException
//	{
//
//	}
//
//	@Override
//	public IReadContext getReadContext()
//	{
//		return null;
//	}
//
//	@Override
//	public void close()
//	{
//		send(new CloseMessage());
//	}
//
//	@Override
//	public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds)
//	{
//		return send(new DeleteMessage(accountId, folderId, messageIds));
//	}
//
//	@Override
//	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
//										Collection<String> messageIds, String targetFolderId, boolean delete)
//	{
//		return send(new MoveToJunkMessage(accountId, sourceFolderId, messageIds, targetFolderId, delete));
//	}
//
//	@Override
//	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
//								  String targetFolderId, boolean delete)
//	{
//		return send(new CopyMessage(accountId, sourceFolderId, messageIds, targetFolderId, delete));
//	}
//
//	@Override
//	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
//											boolean flag)
//	{
//		return send(new MarkAsAnsweredMessage(accountId, folderId, messageIds, flag));
//	}
//
//	@Override
//	public Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds,
//										boolean flag)
//	{
//		return send(new MarkAsReadMessage(accountId, folderId, messageIds, flag));
//	}
//
//	@Override
//	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
//										boolean flag)
//	{
//		return send(new MarkAsJunkMessage(accountId, folderId, messageIds, flag));
//	}
//
//	@Override
//	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId)
//	{
//		return send(new EnsureContentLoadedMessage(accountId, folderId, messageId));
//	}
//
//	@Override
//	public Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds)
//	{
//		return null;
//	}
}