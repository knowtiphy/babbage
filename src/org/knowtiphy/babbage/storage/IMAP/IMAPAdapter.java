package org.knowtiphy.babbage.storage.IMAP;

import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IdleManager;
import com.sun.mail.util.MailConnectException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.IAdapter;
import org.knowtiphy.babbage.storage.IReadContext;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.ReadContext;
import org.knowtiphy.babbage.storage.StorageException;
import org.knowtiphy.babbage.storage.TransactionRecorder;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.babbage.storage.WriteContext;
import org.knowtiphy.utils.FileUtils;
import org.knowtiphy.utils.JenaUtils;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.FetchProfile;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.FolderClosedException;
import javax.mail.Message;
import javax.mail.MessageRemovedException;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.StoreClosedException;
import javax.mail.UIDFolder;
import javax.mail.event.MessageChangedEvent;
import javax.mail.event.MessageChangedListener;
import javax.mail.event.MessageCountAdapter;
import javax.mail.event.MessageCountEvent;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.search.FlagTerm;
import javax.mail.search.SearchTerm;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static org.knowtiphy.babbage.storage.IMAP.DStore.P;
import static org.knowtiphy.babbage.storage.IMAP.DStore.R;

/**
 * @author graham
 */
public class IMAPAdapter extends BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(IMAPAdapter.class.getName());

	private static final long FREQUENCY = 60000L;
	private static final String JUNK_FLAG = "Junk";

	private static final int NUM_ATTEMPTS = 5;
	private static final Runnable POISON_PILL = () -> {
	};
	private static final Pattern INBOX = Pattern.compile("INBOX");

	private final String serverName;
	private final String emailAddress;
	private final String password;
	private String nickName;
	private final String id;
	private final Set<String> trustedSenders, trustedContentProviders;

	//	RDF ids to Java folder and message objects
	private final Map<String, Folder> m_folder = new HashMap<>(100);
	private final Map<Folder, Map<String, Message>> m_PerFolderMessage = new HashMap<>(1000);

	//	map org.knowtiphy.pinkpigmail.messages to ids of those org.knowtiphy.pinkpigmail.messages for org.knowtiphy.pinkpigmail.messages being deleted
	//private final Map<Message, String> m_toDelete = new HashMap<>();

	private final BlockingQueue<Runnable> workQ;
	//private final BlockingQueue<Runnable> contentQ;
	private final Thread doWork;
	private final ExecutorService doContent;
	private final Mutex accountLock;
	private final Dataset messageDatabase;
	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;
	// Maybe don't this here
	private final Model model;
	private final IdleManager idleManager;
	private final Store store;
	private Thread pingThread;
	private Folder inbox;

	public IMAPAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
					   BlockingDeque<Runnable> notificationQ, Model model)
			throws InterruptedException, MessagingException, IOException
	{
		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.notificationQ = notificationQ;
		this.model = model;

		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

		// Query for these with the passed in model
		this.serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
		this.emailAddress = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		this.password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));
		try
		{
			this.nickName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_NICK_NAME));
		} catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

		this.trustedSenders = new HashSet<>(100);
		JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_TRUSTED_SENDER)
				.forEachRemaining(x -> trustedSenders.add(x.toString()));
		this.trustedContentProviders = new HashSet<>(100);
		JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER)
				.forEachRemaining(x -> trustedContentProviders.add(x.toString()));

		this.id = Vocabulary.E(Vocabulary.IMAP_ACCOUNT, emailAddress);

		accountLock = new Mutex();
		accountLock.lock();

		workQ = new LinkedBlockingQueue<>();
		doWork = new Thread(new Worker(workQ));
		doContent = Executors.newCachedThreadPool();

		doWork.start();

		//	TODO -- all these need to be in RDF
		Properties incoming = new Properties();
		incoming.setProperty("mail.store.protocol", "imaps");
		incoming.setProperty("mail.imaps.host", serverName);
		incoming.setProperty("mail.imaps.compress.enable", "true");
		incoming.setProperty("mail.imaps.usesocketchannels", "true");
		incoming.setProperty("mail.imaps.peek", "true");
		incoming.setProperty("mail.imaps.connectionpoolsize", "10");
		//		incoming.setProperty("mail.imaps.fetchsize", "1000000");
		//		incoming.setProperty("mail.imaps.timeout", timeout + "");
		//		incoming.setProperty("mail.imaps.connectiontimeout", connectiontimeout + "");
		//		incoming.setProperty("mail.imaps.writetimeout", writetimeout + "");
		//		incoming.setProperty("mail.imaps.connectionpooltimeout", connectionpooltimeout + "");
		// incoming.setProperty("mail.imaps.port", "993");

		//	we need system properties to pick up command line flags
		Properties props = System.getProperties();
		props.putAll(incoming);
		Session session = Session.getInstance(props, null);
		//session.setDebug(true);
		store = session.getStore("imaps");

		store.connect(serverName, emailAddress, password);
		//	we can in fact have one idle manager for all accounts ..
		ExecutorService es = Executors.newCachedThreadPool();
		idleManager = new IdleManager(session, es);
		Properties props1 = session.getProperties();
		props1.setProperty("mail.event.scope", "session"); // or "application"
		props1.put("mail.event.executor", es);
	}

	private static void openFolder(Folder folder) throws StorageException
	{
		for (int i = 0; i < NUM_ATTEMPTS; i++)
		{
			try
			{
				folder.open(Folder.READ_WRITE);
				return;
			} catch (IllegalStateException e)
			{
				//	the folder is already open
				return;
			} catch (MessagingException e)
			{
				//	ignore and try again
			}
		}

		throw new StorageException("Failed to re-open folder");
	}

	private static UIDFolder U(Folder folder)
	{
		return (UIDFolder) folder;
	}

	//	Note: setFlags does not fail if one of the org.knowtiphy.pinkpigmail.messages is deleted
	private static void mark(Message[] messages, Flags flags, boolean value) throws MessagingException
	{
		assert messages.length > 0;
		messages[0].getFolder().setFlags(messages, flags, value);
	}

	private static List<Address> toList(String raw, boolean ignoreAddr) throws AddressException
	{
		if (raw == null)
		{
			return new LinkedList<>();
		}

		String trim = raw.trim();
		if (trim.isEmpty())
		{
			return new LinkedList<>();
		}

		String[] tos = trim.split(",");
		List<Address> result = new ArrayList<>(10);
		for (String to : tos)
		{
			try
			{
				result.add(new InternetAddress(to.trim()));
			} catch (AddressException ex)
			{
				if (!ignoreAddr)
				{
					throw (ex);
				}
			}
		}

		return result;
	}

	@Override
	public String getId()
	{
		return id;
	}

	public String getServerName()
	{
		return serverName;
	}

	public String getEmailAddress()
	{
		return emailAddress;
	}

	public String getPassword()
	{
		return password;
	}

	public Set<String> getTrustedSenders()
	{
		return trustedSenders;
	}

	public Set<String> getTrustedContentProviders()
	{
		return trustedContentProviders;
	}

	private void ensureMapsLoaded() throws InterruptedException
	{
		accountLock.lock();
		accountLock.unlock();
	}

	public FutureTask<?> getSynchTask()
	{
		return new FutureTask<Void>(() -> {
			startFolderWatchers();
			TransactionRecorder recorder = new TransactionRecorder();
			synchronizeFolders(recorder);
			notifyListeners(recorder);

			for (Folder folder : m_folder.values())
			{
				TransactionRecorder recorder1 = new TransactionRecorder();
				synchMessageIdsAndHeaders(folder, recorder1);
				notifyListeners(recorder1);
			}

			startPingThread();

			accountLock.unlock();
			LOGGER.log(Level.INFO, "{0} :: SYNCH DONE ", emailAddress);
			return null;
		});
	}

	// @formatter:off
	@Override
	public void addListener() throws UnsupportedOperationException
	{
		Model accountTriples = ModelFactory.createDefaultModel();

		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.RDF_TYPE), P(accountTriples, Vocabulary.IMAP_ACCOUNT));
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_NAME), serverName);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS), emailAddress);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_PASSWORD), password);
		if (nickName != null)
		{
			accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_NICK_NAME), nickName);
		}
		trustedSenders.forEach(x -> accountTriples
				.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_TRUSTED_SENDER), x));
		trustedContentProviders.forEach(x -> accountTriples
				.add(R(accountTriples, id),
						P(accountTriples, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER), x));

		// Notify the client of the account triples
		TransactionRecorder accountRec = new TransactionRecorder();
		accountRec.addedStatements(accountTriples);
		notifyListeners(accountRec);

		IReadContext context = getReadContext();
		context.start();

		// So when this added, query the DB and feed those into client the client via notifying the listener
		String constructQueryFD = DFetch.skeleton(id);
		Model mFD = QueryExecutionFactory.create(constructQueryFD, context.getModel()).execConstruct();
		TransactionRecorder rec = new TransactionRecorder();
		rec.addedStatements(mFD);
		notifyListeners(rec);

		String constructQueryMH = DFetch.initialState(id);
		Model mMH = QueryExecutionFactory.create(constructQueryMH, context.getModel()).execConstruct();
		TransactionRecorder recMH = new TransactionRecorder();
		recMH.addedStatements(mMH);
		notifyListeners(recMH);

		context.end();
	}
	// @formatter:on

	public void close()
	{
		LOGGER.log(Level.INFO, "{0} :: shutting down work queues", emailAddress);
		try
		{
			workQ.add(POISON_PILL);
			doWork.join();
			doContent.shutdown();
			doContent.awaitTermination(10_000L, TimeUnit.SECONDS);
		} catch (InterruptedException ex)
		{
			//  ignore
		}

		LOGGER.log(Level.INFO, "{0} :: shutting down idle manager", emailAddress);
		idleManager.stop();

		LOGGER.log(Level.INFO, "{0} :: shutting down ping thread", emailAddress);
		if (pingThread != null)
		{
			pingThread.interrupt();
		}

		//		for (Folder folder : m_folder.values())
		//		{
		//			logger.info(emailAddress + " :: " + folder.getName() + " :: closing ");
		//			try
		//			{
		//				//Platform.runLater(() -> message.set(emailAddress + " :: " + folder.getName() + " :: closing "));
		//				folder.close();
		//			} catch (Exception $)
		//			{
		//				//	ignore
		//			}
		//		}

		LOGGER.log(Level.INFO, "{0} :: closing store", emailAddress);
		try
		{
			store.close();
		} catch (MessagingException ex)
		{
			//	ignore
		}
	}

	//	AUDIT -- I think this one is ok
	public Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag)
	{
		return addWork(new MessageWork(() -> {
			Message[] messages = U(folderId, messageIds);
			mark(messages, new Flags(Flags.Flag.ANSWERED), flag);
			return messages[0].getFolder();
		}));
	}

	//	AUDIT -- I think this one is ok
	public Future<?> markMessagesAsRead(Collection<String> messageIds, String folderId, boolean flag)
	{
		return addWork(new MessageWork(() -> {
			Message[] messages = U(folderId, messageIds);
			mark(messages, new Flags(Flags.Flag.SEEN), flag);
			return messages[0].getFolder();
		}));
	}

	//	AUDIT -- I think this one is ok
	public Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag)
	{
		return addWork(new MessageWork(() -> {
			Message[] messages = U(folderId, messageIds);
			mark(messages, new Flags(JUNK_FLAG), flag);
			return messages[0].getFolder();
		}));
	}

	//  TOOD -- got to go since its just a composition of mark junk and copy org.knowtiphy.pinkpigmail.messages -- merge with move
	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
										boolean delete)
	{
		return addWork(() -> {
			LOGGER.log(Level.INFO, "moveMessagesToJunk : {0}", delete);
			Message[] messages = U(sourceFolderId, messageIds);
			Folder sourceFolder = m_folder.get(sourceFolderId);
			assert sourceFolder != null;
			Folder targetFolder = m_folder.get(targetFolderId);
			assert targetFolder != null;
			mark(messages, new Flags(JUNK_FLAG), true);
			sourceFolder.copyMessages(messages, targetFolder);
			if (delete)
			{
				//					for (String mID : messageIds)
				//					{
				//						logger.info("moveMessagesToJunk " + mID);
				//						assert m_PerFolderMessage.get(sourceFolder).get(mID) != null;
				//						m_toDelete.put(m_PerFolderMessage.get(sourceFolder).get(mID), mID);
				//					}
				mark(messages, new Flags(Flags.Flag.DELETED), true);
				sourceFolder.expunge();
			}

			reWatchFolder(sourceFolder);
			reWatchFolder(targetFolder);
			return null;
		});
	}

	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								  boolean delete)
	{
		Folder source = m_folder.get(sourceFolderId);
		assert source != null;
		Folder target = m_folder.get(targetFolderId);
		assert target != null;

		//		for (String mID : messageIds)
		//		{
		////			assert m_message.get(mID) != null;
		////			m_toDelete.put(m_message.get(mID), mID);
		//			System.out.println(m_folder.get(sourceFolderId));
		//			assert m_PerFolderMessage.get(m_folder.get(sourceFolderId)).get(mID) != null;
		//			m_toDelete.put(m_PerFolderMessage.get(m_folder.get(sourceFolderId)).get(mID), mID);
		//		}

		return addWork(new MessageWork(() -> {
			//  TODO -- this is wrong, since can have both source and target close/fail
			Message[] messages = U(sourceFolderId, messageIds);
			source.copyMessages(messages, target);
			//  is this even necessary, or is the semantics of copy already a delete in the original folder?
			if (delete)
			{
				mark(messages, new Flags(Flags.Flag.DELETED), true);
				source.expunge();
			}
			return source;
		}));
	}

	//	AUDIT -- I DONT think this is correct -- we can mark a message deleted as many times as we
	//	like, and if the expunge fails, we can therefore just retry?
	public Future<?> deleteMessages(String folderId, Collection<String> messageIds)
	{
		Folder folder = m_folder.get(folderId);
		assert folder != null;
		//		for (String mID : messageIds)
		//		{
		//			assert m_PerFolderMessage.get(folder).get(mID) != null;
		//			m_toDelete.put(m_PerFolderMessage.get(folder).get(mID), mID);
		//		}

		return addWork(new MessageWork(() -> {
			Message[] messages = U(folderId, messageIds);
			mark(messages, new Flags(Flags.Flag.DELETED), true);
			messages[0].getFolder().expunge();
			return messages[0].getFolder();
		}));
	}

	public Future<?> appendMessages(String folderId, Message[] messages)
	{
		return addWork(new MessageWork(() -> {
			m_folder.get(folderId).appendMessages(messages);
			return m_folder.get(folderId);
		}));
	}

	//	re-open a closed folder, re-synchronizing the org.knowtiphy.pinkpigmail.messages in the folder and rebuilding
	//	the per folder message map
	//	TODO -- what about the m_delete map?

	public Message createMessage(MessageModel model) throws MessagingException, IOException
	{
		Message message = createMessage();

		//			ReadContext context = getReadContext();
		//			context.start();
		//			try
		//			{
		//				ResultSet resultSet = QueryExecutionFactory
		//						.create(DFetch.outboxMessage(accountId, messageId), context.getModel()).execSelect();
		//				QuerySolution s = resultSet.next();
		//				recipients = s.get(Vars.VAR_TO) == null ? null : s.get(Vars.VAR_TO).asLiteral().getString();
		//				ccs = s.get("cc") == null ? null : s.get("cc").asLiteral().getString();
		//				subject = s.get(Vars.VAR_SUBJECT) == null ? null : s.get(Vars.VAR_SUBJECT).asLiteral().getString();
		//				content = s.get(Vars.VAR_CONTENT) == null ? null : s.get(Vars.VAR_CONTENT).asLiteral().getString();
		//			} finally
		//			{
		//				context.end();
		//			}

		message.setFrom(new InternetAddress(getEmailAddress()));
		message.setReplyTo(new Address[]{new InternetAddress(getEmailAddress())});
		//  TODO -- get rid of the toList stuff
		if (model.getTo() != null)
		{
			for (Address address : toList(model.getTo(), true))
			{
				try
				{
					message.addRecipient(Message.RecipientType.TO, address);
				} catch (AddressException ex)
				{
					//  ignore
				}
			}
		}

		if (model.getCc() != null)
		{
			for (Address address : toList(model.getCc(), true))
			{
				try
				{
					message.addRecipient(Message.RecipientType.CC, address);
				} catch (AddressException ex)
				{
					//  ignore
				}
			}
		}

		if (model.getSubject() != null)
		{
			message.setSubject(model.getSubject());
		}
		//  have to have non null content
		((Multipart) message.getContent()).getBodyPart(0)
				.setContent(model.getContent() == null ? "" : model.getContent(), model.getMimeType());

		//	setup attachments
		for (Path path : model.getAttachments())
		{
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			DataSource source = new FileDataSource(path.toFile());
			messageBodyPart.setDataHandler(new DataHandler(source));
			messageBodyPart.setFileName(FileUtils.baseName(path.toFile()));
			((Multipart) message.getContent()).addBodyPart(messageBodyPart);
		}

		return message;
	}

	//	AUDIT -- I think this will now work if we have a folder closed exception and we re-run the work.
	public Future<?> ensureMessageContentLoaded(String messageId, String folderId)
	{
		return doContent.submit(new MessageWork(() -> {
			ensureMapsLoaded();

			//	check if the content is not already in the database
			ReadContext rContext = getReadContext();
			rContext.start();
			boolean notThere = !rContext.getModel().listObjectsOfProperty(rContext.getModel().createResource(messageId),
					rContext.getModel().createProperty(Vocabulary.HAS_CONTENT)).hasNext();
			rContext.end();
			System.err.println("Not there = " + notThere);

			if (notThere)
			{
				Folder folder = m_folder.get(folderId);

				Message[] msgs = U(encode(folder), List.of(messageId));

				//	how do we set a profile for the actual content?
				//				FetchProfile fp = new FetchProfile();
				//				fp.add(FetchProfile.Item.CONTENT_INFO);
				//				folder.fetch(msgs, fp);

				System.err.println("Not there = AAAAA");
				assert m_folder.containsKey(folderId);

				System.err.println("Not there = BBBBB");
				WriteContext context = getWriteContext();
				context.startTransaction();

				try
				{
					//Message message = m_PerFolderMessage.get(folder).get(messageId);
					for (Message message : msgs)
					{
						MessageContent messageContent = new MessageContent(message, true).process();
						DStore.messageContent(context.getModel(), this, message, messageContent);
					}
					context.succeed();
					System.err.println("Not there = CCCCC");

					return folder;
				} catch (IOException | MessagingException ex)
				{
					context.fail(ex);
				}
			}

			return null;
		}));
	}

	private void startPingThread()
	{
		if (inbox != null)
		{
			LOGGER.log(Level.INFO, "Starting ping server for {0}", inbox.getName());
			pingThread = new Thread(new PingServer(this, inbox, FREQUENCY));
			pingThread.start();
		}
	}

	protected void reStartPingThread() throws InterruptedException, MessagingException, StorageException
	{
		if (!store.isConnected())
		{
			reconnect();
		}
		else if (inbox != null && !inbox.isOpen())
		{
			reOpenFolder(inbox);
			startPingThread();
		}
	}

	private void reOpenFolder(Folder folder) throws MessagingException, StorageException, InterruptedException
	{
		LOGGER.log(Level.INFO, "reOpenFolder :: {0} :: {1}", new Object[]{folder.getName(), folder.isOpen()});
		//assert !folder.isOpen();

		accountLock.lock();
		try
		{
			openFolder(folder);
			TransactionRecorder recorder = new TransactionRecorder();
			m_PerFolderMessage.remove(folder);
			synchMessageIdsAndHeaders(folder, recorder);
			notifyListeners(recorder);
		} finally
		{
			accountLock.unlock();
		}
	}

	private void reconnect() //throws MessagingException
	{
		//	TODO -- have to work out how to do this -- leave till later
		LOGGER.log(Level.INFO, "RECONNECTING WITH NO RECOVERY :: {0}", emailAddress);
		//		if (store.isConnected())
		//		{
		//			store.close();
		//		}
		//		store.connect(serverName, emailAddress, password);
		//		logger.info("END RECONNECTING");
		//		for (Folder folder : folders)
		//		{
		//			logger.info("REOPENING :: " + folder.getName());
		//			if (folder.isOpen())
		//			{
		//				folder.close();
		//			}
		//
		//			folder.open(javax.mail.Folder.READ_WRITE);
		//			logger.info("END REOPENING");
		//		}
	}

	public String encode(Folder folder) throws MessagingException
	{
		return Vocabulary.E(Vocabulary.IMAP_FOLDER, getEmailAddress(), U(folder).getUIDValidity());
	}

	//	open a folder -- try multiple times just in case it doesn't work

	protected String encode(Message message) throws MessagingException
	{
		return Vocabulary.E(Vocabulary.IMAP_MESSAGE, getEmailAddress(), U(message.getFolder()).getUIDValidity(),
				U(message.getFolder()).getUID(message));
	}

	protected String encode(Message message, String cidName) throws MessagingException
	{
		return Vocabulary
				.E(Vocabulary.IMAP_MESSAGE_CID_PART, getEmailAddress(), U(message.getFolder()).getUIDValidity(),
						U(message.getFolder()).getUID(message), cidName);
	}

	private WriteContext getWriteContext()
	{
		return new WriteContext(messageDatabase);
	}

	private void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.addLast(() -> listenerManager.notifyChangeListeners(recorder));
	}

	private void reWatchFolder(Folder folder) throws StorageException
	{
		LOGGER.log(Level.INFO, "REWATCH :: {0}", folder.getName());
		assert idleManager != null;

		try
		{
			idleManager.watch(folder);
			return;
		} catch (MessagingException ex)
		{
			for (int i = 0; i < NUM_ATTEMPTS; i++)
			{
				try
				{
					LOGGER.log(Level.INFO, "REWATCH :: {0} :: {1}", new Object[]{i, folder.getName()});
					reOpenFolder(folder);
					idleManager.watch(folder);
					return;
				} catch (InterruptedException | MessagingException e)
				{
					//	ignore and try again -- all the exceptions are folder related
				}
			}
		}

		throw new StorageException("Failed to re-watch folder");
	}

	private void store(Model model, Folder folder) throws MessagingException
	{
		String folderName = encode(folder);
		Resource folderRes = model.createResource(folderName);
		model.add(folderRes, model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.IMAP_FOLDER));
		model.add(model.createResource(getId()), model.createProperty(Vocabulary.CONTAINS), folderRes);model.add(folderRes, model.createProperty(Vocabulary.HAS_UID_VALIDITY),
				model.createTypedLiteral(((UIDFolder) folder).getUIDValidity()));
		model.add(folderRes, model.createProperty(Vocabulary.HAS_NAME), model.createTypedLiteral(folder.getName()));
		DStore.folderCounts(model, this, folder);
	}

	//	AUDIT -- safe
	private Message[] U(String folderId, Collection<String> messageIds)
	{
		Message[] messages = new Message[messageIds.size()];
		int i = 0;
		Folder folder = m_folder.get(folderId);
		for (String message : messageIds)
		{
			assert m_PerFolderMessage.get(folder).get(message) != null;
			messages[i] = m_PerFolderMessage.get(folder).get(message);
			i++;
		}

		return messages;
	}

	private ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

	public <T> Future<T> addWork(Callable<T> operation)
	{
		FutureTask<T> task = new FutureTask<>(operation);
		workQ.add(task);
		return task;
	}

	private MimeMessage createMessage() throws MessagingException
	{
		//	we need system properties to pick up command line flags
		Properties outGoing = System.getProperties();
		//  TODO -- need to get this from the database and per account
		outGoing.setProperty("mail.transport.protocol", "smtp");
		outGoing.setProperty("mail.smtp.host", "chimail.midphase.com");
		outGoing.setProperty("mail.smtp.ssl.enable", "true");
		outGoing.setProperty("mail.smtp.port", "465");
		//	do I need this?
		outGoing.setProperty("mail.smtp.auth", "true");
		//	TODO -- can do this another way?
		Properties props = System.getProperties();
		props.putAll(outGoing);
		Session session = Session.getInstance(props, new Authenticator()
		{
			@Override
			protected PasswordAuthentication getPasswordAuthentication()
			{
				return new PasswordAuthentication(getEmailAddress(), getPassword());
			}
		});

		MimeMessage message = new MimeMessage(session);
		MimeMultipart multipart = new MimeMultipart();
		MimeBodyPart body = new MimeBodyPart();
		multipart.addBodyPart(body);
		message.setContent(multipart);
		return message;
	}

	private void startFolderWatchers() throws MessagingException
	{
		// Each account needs to have its folders now
		// start a watcher for each one
		assert m_folder.isEmpty();

		for (Folder folder : store.getDefaultFolder().list())//"*"))
		{
			try
			{
				openFolder(folder);
				m_folder.put(encode(folder), folder);
				if (INBOX.matcher(folder.getName()).matches())
				{
					inbox = folder;
				}
			} catch (MessagingException | StorageException ex)
			{
				//ex.printStackTrace();
				LOGGER.info(folder.getName());
			}
		}

		for (Folder folder : m_folder.values())
		{
			LOGGER.log(Level.INFO, "Starting watcher for {0}", folder.getName());
			folder.addMessageCountListener(new WatchCountChanges(this, folder));
			folder.addMessageChangedListener(new WatchMessageChanges(this, folder));
			try
			{
				idleManager.watch(folder);
			} catch (MessagingException e)
			{
				LOGGER.warning(e.getLocalizedMessage());
			}
		}
	}

	private void synchronizeFolders(TransactionRecorder recorder) throws MessagingException
	{
		LOGGER.info("synchronizeFolders");

		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		try
		{
			for (Folder folder : m_folder.values())
			{
				String folderName = encode(folder);

				Model model = context.getModel();

				//  store any folders we don't already have, and update folder counts for ones we do have
				//  TODO -- need to handle deleted folders
				StmtIterator it = model
						.listStatements(model.createResource(folderName), model.createProperty(Vocabulary.RDF_TYPE),
								model.createResource(Vocabulary.IMAP_FOLDER));

				if (it.hasNext())
				{
					//  TODO -- should really check that the validity hasn't changed
					assert JenaUtils.checkUnique(it);
					//	folder counts may have changed
					DStore.folderCounts(model, this, folder);
				}
				else
				{
					System.err.println("ADDING FOLDER " + folder.getName());
					store(model, folder);
				}
			}

			context.succeed();
		} catch (MessagingException ex)
		{
			context.fail(ex);
		}
	}

	//  get the ids of those org.knowtiphy.pinkpigmail.messages stored in the database that match the query
	private Set<String> getStoredMessageIDs(String query)
	{
		Set<String> stored = new HashSet<>(1000);
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			stored.addAll(JenaUtils.set(resultSet, soln -> soln.get("message").asResource().toString()));
		} finally
		{
			messageDatabase.end();
		}

		return stored;
	}

	//	TODO -- should combine methods
	private void synchMessageIdsAndHeaders(Folder folder, TransactionRecorder recorder) throws MessagingException
	{
		synchMessageIds(folder, recorder);
		synchMessageHeaders(folder, recorder);
	}

	private void synchMessageIds(Folder folder, TransactionRecorder recorder) throws MessagingException
	{
		LOGGER.log(Level.INFO, "synchMessageIds {0}", folder.getName());

		//  get the stored message IDs
		Set<String> stored = getStoredMessageIDs(DFetch.messageUIDs(encode(folder)));

		//  get the set of message IDst that IMAP reports
		//  TODO -- this is dumb, sequence is bad
		//        if (!folder.isOpen())
		//        {
		//            folder.open(javax.mail.Folder.READ_WRITE);
		//        }
		SearchTerm st = new FlagTerm(new Flags(Flags.Flag.DELETED), false);
		Message[] msgs = folder.search(st);
		FetchProfile fp = new FetchProfile();
		fp.add(UIDFolder.FetchProfileItem.UID);
		folder.fetch(msgs, fp);

		Map<String, Message> perMessageMap = new HashMap<>(1000);

		//  we need to add org.knowtiphy.pinkpigmail.messages in the set (folder - stored)
		Collection<String> addUID = new HashSet<>(1000);
		for (Message msg : msgs)
		{
			String messageName = encode(msg);

			// implementation of perFolderMessageMap
			perMessageMap.put(messageName, msg);

			if (!stored.contains(messageName))
			{
				//System.out.println("Adding a message");
				addUID.add(messageName);
			}
		}

		// Associate the folder ID to a map of org.knowtiphy.pinkpigmail.messages
		m_PerFolderMessage.put(folder, perMessageMap);

		//  we need to remove org.knowtiphy.pinkpigmail.messages in the set (stored - folder) - stored - m_message,keys() at this point
		Collection<String> removeUID = new HashSet<>(1000);
		for (String messageName : stored)
		{
			if (!m_PerFolderMessage.get(folder).containsKey(messageName))
			{
				removeUID.add(messageName);
			}
		}

		//  delete any stored message whose ID is not in the set of message IDs for the folder
		//        logger.info("TO ADD = " + addUID.size());
		//        logger.info("TO REMOVE = " + removeUID.size());
		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		try
		{
			for (String message : removeUID)
			{
				DStore.unstoreMessage(messageDatabase.getDefaultModel(), encode(folder), message);
			}
			for (String message : addUID)
			{
				DStore.messageID(messageDatabase.getDefaultModel(), encode(folder), message);
			}

			context.succeed();
		} catch (MessagingException ex)
		{
			context.fail(ex);
		}
	}

	private void synchMessageHeaders(Folder folder, TransactionRecorder recorder) throws MessagingException
	{
		//  get the stored message IDs
		Set<String> stored = getStoredMessageIDs(DFetch.messageUIDs(encode(folder)));
		//  get the stored message IDs for those org.knowtiphy.pinkpigmail.messages for which we have headers
		Set<String> withHeaders = getStoredMessageIDs(DFetch.messageUIDsWithHeaders(id, encode(folder)));
		//  get message headers for message ids that we don't have headers for
		stored.removeAll(withHeaders);

		if (!stored.isEmpty())
		{
			Message[] msgs = U(encode(folder), stored);
			FetchProfile fp = new FetchProfile();
			fp.add(FetchProfile.Item.ENVELOPE);
			//	TODO: headers vs envelope?
			fp.add(IMAPFolder.FetchProfileItem.HEADERS);
			fp.add(FetchProfile.Item.FLAGS);
			folder.fetch(msgs, fp);

			//  fetch headers we don't have
			WriteContext context = getWriteContext();
			context.startTransaction(recorder);
			try
			{
				for (Message msg : msgs)
				{
					DStore.messageHeaders(messageDatabase.getDefaultModel(), msg, encode(msg));
				}

				context.succeed();
			} catch (MessagingException ex)
			{
				context.fail(ex);
			}
		}
	}

	// Handles incoming org.knowtiphy.pinkpigmail.messages from the IMAP org.knowtiphy.pinkpigmail.server, new ones not caught in the synch of initial start up state
	private class WatchMessageChanges implements MessageChangedListener
	{
		private final IAdapter account;
		private final Folder folder;

		WatchMessageChanges(IAdapter account, Folder folder)
		{
			this.account = account;
			this.folder = folder;
		}

		private boolean isDeleted(Message message) throws MessagingException
		{
			try
			{
				//	do anything that can cause a MessageRemovedException
				message.isSet(Flags.Flag.SEEN);
				return false;
			} catch (MessageRemovedException ex)
			{
				return true;
			}
		}

		@Override
		public void messageChanged(MessageChangedEvent messageChangedEvent)
		{
			LOGGER.log(Level.INFO, "HAVE A MESSAGE CHANGED {0}", messageChangedEvent);
			LOGGER.log(Level.INFO, "{0}THREAD IS = ", Thread.currentThread().getId());

			Message message = messageChangedEvent.getMessage();

			addWork(new MessageWork(() -> {
				if (messageChangedEvent.getMessageChangeType() == MessageChangedEvent.FLAGS_CHANGED)
				{
					//	deletes are handled elsewhere
					if (!isDeleted(message))
					{
						WriteContext context = getWriteContext();
						context.startTransaction();
						try
						{
							DStore.folderCounts(context.getModel(), account, folder);
							String messageName = encode(message);
							if (m_PerFolderMessage.get(folder).containsKey(messageName))
							{
								DStore.flags(context.getModel(), messageName, message);
							}
							else
							{
								//  not sure if this is possible -- probably not
								LOGGER.info("OUT OF ORDER CHANGE");
							}
							context.succeed();
							notifyListeners(context.getRecorder());
						} catch (MessagingException ex)
						{
							context.fail(ex);
						}
					}
				}
				else
				{
					LOGGER.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX---WTF -- the envelope changed????");
				}

				return folder;
			}));
		}
	}

	private class WatchCountChanges extends MessageCountAdapter
	{
		private final IAdapter account;
		private final Folder folder;

		WatchCountChanges(IAdapter account, Folder folder)
		{
			this.account = account;
			this.folder = folder;
		}

		@Override
		public void messagesRemoved(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "WatchCountChanges::messagesRemoved {0}", e.getMessages().length);
			LOGGER.log(Level.INFO, "{0}THREAD  = ", Thread.currentThread().getId());

			addWork(new MessageWork(() -> {
				WriteContext context = getWriteContext();
				context.startTransaction();

				try
				{
					DStore.folderCounts(context.getModel(), account, folder);
					for (Message message : e.getMessages())
					{
						//	delete event for a delete message that a client connected to this org.knowtiphy.pinkpigmail.server initiated
						//						if (m_toDelete.containsKey(message))
						//						{
						//							String messageName = m_toDelete.get(message);
						//							m_PerFolderMessage.get(folder).remove(messageName);
						//							m_toDelete.remove(message);
						//							DStore.unstoreMessage(messageDatabase.getDefaultModel(), encode(folder), messageName);
						//						}
						//						else
						//						{
						DStore.unstoreMessage(context.getModel(), encode(folder), encode(message));
					}

					context.succeed();
					notifyListeners(context.getRecorder());
					return folder;
				} catch (MessagingException ex)
				{
					context.fail(ex);
					return null;
				}
			}));
		}

		@Override
		public void messagesAdded(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "HAVE A MESSAGE ADDED {0}", Arrays.toString(e.getMessages()));
			LOGGER.log(Level.INFO, "THREAD IS = {0}", Thread.currentThread().getId());

			addWork(new MessageWork(() -> {

				WriteContext context = getWriteContext();
				context.startTransaction();
				try
				{
					Model model = context.getModel();
					DStore.folderCounts(model, account, folder);
					for (Message message : e.getMessages())
					{
						String messageName = encode(message);
						DStore.messageID(context.getModel(), encode(folder), messageName);
						DStore.messageHeaders(context.getModel(), message, messageName);
						m_PerFolderMessage.get(folder).put(messageName, message);
					}

					context.succeed();
					notifyListeners(context.getRecorder());
					return folder;
				} catch (MessagingException ex)
				{
					context.fail(ex);
					return null;
				}
			}));
		}
	}

	private class MessageWork implements Callable<Folder>
	{
		private final Callable<? extends Folder> work;

		MessageWork(Callable<? extends Folder> work)
		{
			this.work = work;
		}

		@Override
		public Folder call() throws Exception
		{
			for (int attempts = 0; attempts < NUM_ATTEMPTS; attempts++)
			{
				try
				{
					Folder folder = work.call();
					if (folder != null)
					{
						//	TODO -- what happens if the rewatch fails? Can it fail?
						reWatchFolder(folder);
					}
					return null;
				} catch (StoreClosedException ex)
				{
					reconnect();
				} catch (FolderClosedException ex)
				{
					reOpenFolder(ex.getFolder());
				} catch (MailConnectException ex)
				{
					//  TODO -- timeout -- not really sure what this does
					LOGGER.info("TIMEOUT -- adapting timeout");
					//timeout = timeout * 2;
				}

				LOGGER.log(Level.INFO, "MessageWork::call RE-ATTEMPT {0}", attempts);

				//	TODO -- no idea how to recover from other kinds of exceptions
			}

			throw new StorageException("OPERATION FAILED");
		}
	}

	private class Worker implements Runnable
	{
		private final BlockingQueue<? extends Runnable> queue;

		Worker(BlockingQueue<? extends Runnable> queue)
		{
			this.queue = queue;
		}

		@Override
		public void run()
		{
			while (true)
			{
				try
				{
					Runnable task = queue.take();
					if (task == POISON_PILL)
					{
						return;
					}
					else
					{
						ensureMapsLoaded();
						try
						{
							task.run();
						} catch (RuntimeException ex)
						{
							LOGGER.warning(ex.getLocalizedMessage());
						}
					}
				} catch (InterruptedException e)
				{
					return;
				}
			}
		}
	}

}