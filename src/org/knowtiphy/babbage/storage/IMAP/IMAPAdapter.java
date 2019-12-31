package org.knowtiphy.babbage.storage.IMAP;

import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IMAPStore;
import com.sun.mail.imap.IdleManager;
import com.sun.mail.util.MailConnectException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.IAdapter;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.StorageException;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.FileUtils;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.LoggerUtils;

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static org.knowtiphy.babbage.storage.IMAP.DStore.P;
import static org.knowtiphy.babbage.storage.IMAP.DStore.R;
import static org.knowtiphy.babbage.storage.IMAP.DStore.addFolder;
import static org.knowtiphy.babbage.storage.IMAP.DStore.addFolderCounts;
import static org.knowtiphy.babbage.storage.IMAP.DStore.addMessageContent;
import static org.knowtiphy.babbage.storage.IMAP.DStore.addMessageHeaders;
import static org.knowtiphy.babbage.storage.IMAP.DStore.deleteMessageFlags;
import static org.knowtiphy.utils.IProcedure.doAndIgnore;

/**
 * @author graham
 */
public class IMAPAdapter extends BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(IMAPAdapter.class.getName());

	private final String id;
	private final String serverName;
	private final String emailAddress;
	private final String password;
	private String nickName;
	private final Set<String> trustedSenders, trustedContentProviders;

	//	RDF ids to javax mail folder and message objects
	private final Map<String, Folder> m_folder = new HashMap<>(100);
	private final Map<Folder, Map<String, Message>> m_PerFolderMessage = new HashMap<>(1000);

	//	map org.knowtiphy.pinkpigmail.messages to ids of those org.knowtiphy.pinkpigmail.messages for org.knowtiphy.pinkpigmail.messages being deleted
	//private final Map<Message, String> m_toDelete = new HashMap<>();

	private final Store store;
	private final IdleManager idleManager;
	private Thread pingThread;

	private final ExecutorService workService;
	//	these are really the same thing, but having two allows fetching the content of a message immediately, even
	//	if the load ahead service is completely busy.
	private final ExecutorService contentService;
	private final ExecutorService loadAheadService;

	private final Mutex accountLock;

	//	special folders
	Folder archive;
	Folder drafts;
	Folder inbox;
	Folder junk;
	Folder sent;
	Folder trash;

	public IMAPAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
					   BlockingDeque<Runnable> notificationQ, Model model)
			throws InterruptedException, MessagingException, IOException
	{
		super(messageDatabase, listenerManager, notificationQ);

		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

		this.serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
		this.emailAddress = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		this.password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));
		this.id = Vocabulary.E(Vocabulary.IMAP_ACCOUNT, emailAddress);

		try
		{
			this.nickName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_NICK_NAME));
		} catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

		this.trustedSenders = new HashSet<>(100);
		JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_TRUSTED_SENDER).forEachRemaining(x -> trustedSenders.add(x.toString()));
		this.trustedContentProviders = new HashSet<>(100);
		JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER).forEachRemaining(x -> trustedContentProviders.add(x.toString()));

		accountLock = new Mutex();
		accountLock.lock();

		workService = Executors.newSingleThreadExecutor();
		contentService = Executors.newFixedThreadPool(4);
		loadAheadService = Executors.newFixedThreadPool(4);

		Properties incoming = new Properties();
		incoming.put("mail.store.protocol", "imaps");
		incoming.put("mail.imaps.host", serverName);
		incoming.put("mail.imaps.usesocketchannels", "true");
		incoming.put("mail.imaps.peek", "true");
		//	TODO -- all these need to be in RDF
		incoming.put("mail.imaps.compress.enable", "true");
		incoming.put("mail.imaps.connectionpoolsize", "20");
		//	TODO -- what is a sane number for this?
		incoming.put("mail.imaps.fetchsize", "3000000");
		incoming.setProperty("mail.imaps.connectiontimeout", "5000");
		incoming.setProperty("mail.imaps.timeout", "5000");
		// incoming.setProperty("mail.imaps.port", "993");

		//	we need system properties to pick up command line flags
		Properties props = System.getProperties();
		props.putAll(incoming);
		Session session = Session.getInstance(props, null);
//		session.setDebug(true);

		Properties props1 = session.getProperties();
		//props1.put("mail.event.scope", "session"); // or "application"
		ExecutorService es = Executors.newCachedThreadPool();
		props1.put("mail.event.executor", es);

		store = session.getStore("imaps");
		store.connect(serverName, emailAddress, password);
		//	we can in fact have one idle manager for all accounts ..
		idleManager = new IdleManager(session, es);

//		System.out.println("CAPABILITES ----------------------- " + nickName);
//		System.out.println(((IMAPStore) store).hasCapability("LIST-EXTENDED"));
//		System.out.println(((IMAPStore) store).hasCapability("SPECIAL-USE"));
	}

	@Override
	public String getId()
	{
		return id;
	}

	public FutureTask<?> getSynchTask()
	{
		return new FutureTask<Void>(() -> {
			startFolderWatchers();
			computeSpecialFolders();
			synchronizeFolders();

			for (Folder folder : m_folder.values())
			{
				synchMessageIdsAndHeaders(folder);
			}

			startPingThread();

			accountLock.unlock();
			LOGGER.log(Level.INFO, "{0} :: SYNCH DONE ", emailAddress);
			return null;
		});
	}

	@Override
	public void addListener()
	{
		Delta delta = new Delta()
				.addR(id, Vocabulary.RDF_TYPE, Vocabulary.IMAP_ACCOUNT)
				.addL(id, Vocabulary.HAS_SERVER_NAME, serverName)
				.addL(id, Vocabulary.HAS_EMAIL_ADDRESS, emailAddress)
				.addL(id, Vocabulary.HAS_PASSWORD, password)
				.addLN(id, Vocabulary.HAS_NICK_NAME, nickName);
		trustedSenders.forEach(x -> delta.addL(id, Vocabulary.HAS_TRUSTED_SENDER, x));
		trustedContentProviders.forEach(x -> delta.addL(id, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER, x));
		notifyListeners(delta);

		queryAndNotify(d ->
				d.add(QueryExecutionFactory.create(DFetch.skeleton(id), messageDatabase.getDefaultModel()).execConstruct()));
		queryAndNotify(d ->
				d.add(QueryExecutionFactory.create(DFetch.initialState(id), messageDatabase.getDefaultModel()).execConstruct()));
	}

	public void close()
	{
		LOGGER.log(Level.INFO, "{0} :: shutting down worker pools", emailAddress);

		doAndIgnore(workService::shutdown);
		doAndIgnore(() -> workService.awaitTermination(10, TimeUnit.SECONDS));
		doAndIgnore(contentService::shutdown);
		doAndIgnore(() -> contentService.awaitTermination(10, TimeUnit.SECONDS));
		doAndIgnore(loadAheadService::shutdown);
		doAndIgnore(() -> loadAheadService.awaitTermination(10, TimeUnit.SECONDS));

		LOGGER.log(Level.INFO, "{0} :: shutting down idle manager", emailAddress);
		idleManager.stop();

		LOGGER.log(Level.INFO, "{0} :: shutting down ping thread", emailAddress);
		if (pingThread != null)
		{
			pingThread.interrupt();
		}

		m_folder.values().forEach(folder ->
		{
			LOGGER.info(emailAddress + " :: " + folder.getName() + " :: closing ");
			doAndIgnore(folder::close);
		});

		LOGGER.log(Level.INFO, "{0} :: closing store", emailAddress);
		doAndIgnore(store::close);
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
			mark(messages, new Flags(Constants.JUNK_FLAG), flag);
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
			mark(messages, new Flags(Constants.JUNK_FLAG), true);
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

		message.setFrom(new InternetAddress(emailAddress));
		message.setReplyTo(new Address[]{new InternetAddress(emailAddress)});
		//  TODO -- get rid of the toList stuff
		if (model.getTo() != null)
		{
			for (Address address : toList(model.getTo()))
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
			for (Address address : toList(model.getCc()))
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

	private Folder load(String folderId, Collection<String> messageIds) throws Exception
	{
		ensureMapsLoaded();

		//	TODO -- not sure this is correct -- what happens if we get a delete between checking its stored and
		//	the fetch of the message -- perhaps the single threadedness helps us?

		List<String> needToFetch = new ArrayList<>();
		for (String messageId : messageIds)
		{
			boolean stored = query(() -> messageDatabase.getDefaultModel().
					listObjectsOfProperty(R(messageDatabase.getDefaultModel(), messageId),
							P(messageDatabase.getDefaultModel(), Vocabulary.HAS_CONTENT)).hasNext());
			if (!stored)
			{
				needToFetch.add(messageId);
			}
		}
		System.out.println("load WORKER : " + needToFetch);

		if (!needToFetch.isEmpty())
		{
			assert m_folder.containsKey(folderId);
			Folder folder = m_folder.get(folderId);
			Message[] msgs = U(encode(folder), needToFetch);

			///how do we set a profile for the actual content?
			FetchProfile fp = new FetchProfile();
			fp.add(FetchProfile.Item.CONTENT_INFO);
			folder.fetch(msgs, fp);

			//	get all the data first since we don't want to hold a write lock if the IMAP fetching stalls
			List<MessageContent> contents = new LinkedList<>();
			for (Message message : msgs)
			{
				contents.add(new MessageContent(message, this, true).process());
			}

			apply(delta -> contents.forEach(content -> addMessageContent(delta, content)));

			System.out.println("load WORKER DONE : " + needToFetch);
			return folder;
		}

		return null;
	}

	public Future<?> ensureMessageContentLoaded(String messageId, String folderId)
	{
		return contentService.submit(new MessageWork(() -> load(folderId, List.of(messageId))));
	}

	public Future<?> loadAhead(String folderId, Collection<String> messageIds)
	{
		return loadAheadService.submit(() ->
		{
			try
			{
				load(folderId, messageIds);
			} catch (Exception ex)
			{
				ex.printStackTrace(System.err);
				//	ignore -- any errors will be picked up elsewhere
			}
		});
	}

	//	private methods start here

	private void ensureMapsLoaded() throws InterruptedException
	{
		accountLock.lock();
		accountLock.unlock();
	}

	private static void openFolder(Folder folder) throws StorageException
	{
		for (int i = 0; i < Constants.NUM_ATTEMPTS; i++)
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

	private static List<Address> toList(String raw) throws AddressException
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
			result.add(new InternetAddress(to.trim()));
		}

		return result;
	}

	private void startPingThread()
	{
		if (inbox != null)
		{
			LOGGER.log(Level.INFO, "Starting ping server for {0}", inbox.getName());
			pingThread = new Thread(new PingServer(this, inbox, Constants.FREQUENCY));
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

		System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX REOPENING FOLDER XXXXXXXXXXXXXXXXXXXXXX");
		accountLock.lock();
		try
		{
			openFolder(folder);
			m_PerFolderMessage.remove(folder);
			synchMessageIdsAndHeaders(folder);
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

	//	methods to encode javax mail objects as URIs

	String encode(Folder folder) throws MessagingException
	{
		return Vocabulary.E(Vocabulary.IMAP_FOLDER, emailAddress, U(folder).getUIDValidity());
	}

	String encode(Message message) throws MessagingException
	{
		UIDFolder folder = U(message.getFolder());
		return Vocabulary.E(Vocabulary.IMAP_MESSAGE, emailAddress, folder.getUIDValidity(), folder.getUID(message));
	}

	String encode(Message message, String cidName) throws MessagingException
	{
		UIDFolder folder = U(message.getFolder());
		return Vocabulary.E(Vocabulary.IMAP_MESSAGE_CID_PART, emailAddress, folder.getUIDValidity(), folder.getUID(message), cidName);
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
			for (int i = 0; i < Constants.NUM_ATTEMPTS; i++)
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

	protected <T> Future<T> addWork(Callable<T> operation)
	{
		return workService.submit(() -> {
			try
			{
				ensureMapsLoaded();
				return operation.call();
			} catch (Exception ex)
			{
				LOGGER.warning(ex.getLocalizedMessage());
				return null;
			}
		});

//		FutureTask<T> task = new FutureTask<>(operation);
//		workQ.add(task);
//		return task;
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
				return new PasswordAuthentication(emailAddress, password);
			}
		});

		MimeMessage message = new MimeMessage(session);
		MimeMultipart multipart = new MimeMultipart();
		MimeBodyPart body = new MimeBodyPart();
		multipart.addBodyPart(body);
		message.setContent(multipart);
		return message;
	}

	private void setSpecialFolder(Folder folder, Pattern[] alternatives, Consumer<Folder> setFolder)
	{
		for (Pattern p : alternatives)
		{
			if (p.matcher(folder.getName()).matches())
			{
				setFolder.accept(folder);
				return;
			}
		}
	}

	private void computeSpecialFolders() throws MessagingException
	{
		assert !m_folder.isEmpty();

		inbox = store.getFolder("INBOX");

		if (((IMAPStore) store).hasCapability("SPECIAL-USE"))
		{
			for (Folder folder : m_folder.values())
			{
				for (String attr : ((IMAPFolder) folder).getAttributes())
				{
					if (Constants.ARCHIVES_ATTRIBUTE.matcher(attr).matches())
					{
						archive = folder;
					}
					if (Constants.DRAFTS_ATTRIBUTE.matcher(attr).matches())
					{
						drafts = folder;
					}
					if (Constants.JUNK_ATTRIBUTE.matcher(attr).matches())
					{
						junk = folder;
					}
					if (Constants.SENT_ATTRIBUTE.matcher(attr).matches())
					{
						sent = folder;
					}
					if (Constants.TRASH_ATTRIBUTE.matcher(attr).matches())
					{
						trash = folder;
					}
				}
			}
		}

		//	we run this code to cover two scenarios:
		//	a) we didn't have the SPECIAL-USE capability
		//	b) we did have SPECIAL-USE but not for all special folders

		for (Folder folder : m_folder.values())
		{
			if (archive == null)
			{
				setSpecialFolder(folder, Constants.ARCHIVE_PATTERNS, f -> archive = f);
			}
			if (drafts == null)
			{
				setSpecialFolder(folder, Constants.DRAFT_PATTERNS, f -> drafts = f);
			}
			if (junk == null)
			{
				setSpecialFolder(folder, Constants.JUNK_PATTERNS, f -> junk = f);
			}
			if (sent == null)
			{
				setSpecialFolder(folder, Constants.SENT_PATTERNS, f -> sent = f);
			}
			if (trash == null)
			{
				setSpecialFolder(folder, Constants.TRASH_PATTERNS, f -> trash = f);
			}
		}

//		System.out.println("XXXXXXXXXXXXXXX " + nickName);
//		System.out.println(junk);
//		System.out.println(sent);
//		System.out.println(trash);
//		System.out.println(inbox);
	}

	private void startFolderWatchers() throws MessagingException, StorageException
	{
		// Each account needs to have its folders now
		// start a watcher for each one
		assert m_folder.isEmpty();

		for (Folder folder : store.getDefaultFolder().list())//"*"))
		{
			openFolder(folder);
			m_folder.put(encode(folder), folder);
		}

		for (Folder folder : m_folder.values())
		{
			LOGGER.log(Level.INFO, "Starting watcher for {0}", folder.getName());
			folder.addMessageCountListener(new WatchCountChanges(folder));
			folder.addMessageChangedListener(new WatchMessageChanges(folder));
			idleManager.watch(folder);
		}
	}

	//	synch methods

	private void synchronizeFolders() throws MessagingException
	{
		LOGGER.info("synchronizeFolders");

		//  store any folders we don't already have, and update folder counts for ones we do have
		//  TODO -- need to handle deleted folders

		applyAndNotify(delta -> {
			for (Folder folder : m_folder.values())
			{
				String folderId = encode(folder);
				StmtIterator storedFolder = DFetch.folder(messageDatabase.getDefaultModel(), folderId);
				boolean isStored = storedFolder.hasNext();
				assert !isStored || JenaUtils.checkUnique(storedFolder);

				//	if we have the folder delete its folder counts as they may have changed
				if (isStored)
				{
					//  TODO -- should really check that the validity hasn't changed
					DStore.deleteFolderCounts(messageDatabase.getDefaultModel(), delta, folderId);
				}
				//	add a new folder
				else
				{
					addFolder(delta, this, folder);
				}

				addFolderCounts(delta, folder, folderId);
			}
		});
	}

	private Delta synchMessageIds(Folder folder) throws MessagingException
	{
		LOGGER.log(Level.INFO, "synchMessageIds {0}", folder.getName());

		String folderId = encode(folder);

		//  get the stored message IDs

		Set<String> stored = query(() -> DFetch.messageIds(messageDatabase.getDefaultModel(), DFetch.messageUIDs(folderId)));

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
		Collection<String> removeUID = new HashSet<>(1000);
		Collection<String> addUID = new HashSet<>(1000);

		//  we need to add org.knowtiphy.pinkpigmail.messages in the set (folder - stored)
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
		for (String messageName : stored)
		{
			if (!m_PerFolderMessage.get(folder).containsKey(messageName))
			{
				removeUID.add(messageName);
			}
		}

		return apply(delta ->
		{
			removeUID.forEach(message -> DStore.deleteMessage(messageDatabase.getDefaultModel(), delta, folderId, message));
			addUID.forEach(message -> DStore.addMessage(delta, folderId, message));
		});
	}

	private void synchMessageHeaders(Delta hdrsDelta, Folder folder) throws MessagingException
	{
		String folderId = encode(folder);

		//  get the stored message IDs for those messages for which we have headers
		Set<String> stored = query(() -> DFetch.messageIds(messageDatabase.getDefaultModel(), DFetch.messageUIDs(folderId)));
		//  get message headers for message ids that we don't have headers for
		Set<String> withHeaders = query(() -> DFetch.messageIds(messageDatabase.getDefaultModel(), DFetch.messageUIDsWithHeaders(id, folderId)));

		stored.removeAll(withHeaders);

		//  fetch headers we don't have
		if (!stored.isEmpty())
		{
			Message[] msgs = U(encode(folder), stored);
			FetchProfile fp = new FetchProfile();
			fp.add(FetchProfile.Item.ENVELOPE);
			//	TODO: headers vs envelope?
			fp.add(IMAPFolder.FetchProfileItem.HEADERS);
			fp.add(FetchProfile.Item.FLAGS);
			folder.fetch(msgs, fp);

			hdrsDelta.merge(apply(delta -> {
				delta.merge(hdrsDelta);
				for (Message msg : msgs)
				{
					String messageId = encode(msg);
					addMessageHeaders(delta, msg, messageId);
					deleteMessageFlags(messageDatabase.getDefaultModel(), delta, messageId);
				}
			}));
		}

		notifyListeners(hdrsDelta);
	}

	//	TODO -- should combine methods
	//	todo -- get rid of this crap with hdrsDelta and the merging -- hackery for the UI

	private void synchMessageIdsAndHeaders(Folder folder) throws MessagingException
	{
		Delta hdrsDelta = synchMessageIds(folder);
		synchMessageHeaders(hdrsDelta, folder);
	}

	// handle incoming messages from the IMAP server, new ones not caught in the synch of initial start up state
	private class WatchMessageChanges implements MessageChangedListener
	{
		private Folder folder;

		WatchMessageChanges(Folder folder)
		{
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

			Message message = messageChangedEvent.getMessage();

			folder = (Folder) messageChangedEvent.getSource();
			addWork(new MessageWork(() -> {
				if (messageChangedEvent.getMessageChangeType() == MessageChangedEvent.FLAGS_CHANGED)
				{
					applyAndNotify(delta ->
					{
						String folderId = encode(folder);

						//	message flag changing can indicate that folder counts have changed
						DStore.updateFolderCounts(messageDatabase.getDefaultModel(), delta, folder, folderId);

						//	deletes are handled elsewhere
						if (!isDeleted(message))
						{
							String messageId = encode(message);

							if (m_PerFolderMessage.get(folder).containsKey(messageId))
							{
								deleteMessageFlags(messageDatabase.getDefaultModel(), delta, messageId);
								DStore.addMessageFlags(delta, message, messageId);
							}
							else
							{
								//  not sure if this is possible -- probably not
								LOGGER.info("OUT OF ORDER CHANGE");
							}
//							JenaUtils.printModel(delta.getAdds(), "DELTA");
//							JenaUtils.printModel(delta.getDeletes(), "DELTA");
//							System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
						}
					});
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
		private Folder folder;

		WatchCountChanges(Folder folder)
		{
			this.folder = folder;
		}

		@Override
		public void messagesRemoved(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "WatchCountChanges::messagesRemoved {0}", e.getMessages().length);

			folder = (Folder) e.getSource();
			addWork(new MessageWork(() -> {
				applyAndNotify(delta -> {
					for (Message message : e.getMessages())
					{
						String folderId = encode(folder);
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

						//System.out.println("UPDATING FOLDER COUNTS");
						DStore.updateFolderCounts(messageDatabase.getDefaultModel(), delta, folder, folderId);
						DStore.deleteMessage(messageDatabase.getDefaultModel(), delta, folderId, encode(message));
					}
				});

				return folder;

			}));
		}

		@Override
		public void messagesAdded(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "HAVE A MESSAGE ADDED {0}", Arrays.toString(e.getMessages()));

			folder = (Folder) e.getSource();
			addWork(new MessageWork(() -> {
				applyAndNotify(delta ->
				{
//					System.out.println("MESSAGE ADDED XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");

					String folderId = encode(folder);
					DStore.updateFolderCounts(messageDatabase.getDefaultModel(), delta, folder, folderId);
//					System.out.println("MESSAGE ADDED YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY");

					for (Message message : e.getMessages())
					{
						String messageId = encode(message);
						DStore.addMessage(delta, folderId, messageId);
						DStore.addMessageFlags(delta, message, messageId);
						addMessageHeaders(delta, message, messageId);
						m_PerFolderMessage.get(folder).put(messageId, message);
					}
//					System.out.println("MESSAGE ADDED ZZZZZZZZZZZZZZZZZZZZZZZZ");
				});

				return folder;
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
			for (int attempts = 0; attempts < Constants.NUM_ATTEMPTS; attempts++)
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
				} catch (MessageRemovedException ex)
				{
					System.err.println("XXXXXXXXXXX MW DELETED XXXXXXXX");
					LOGGER.log(Level.INFO, "MessageWork::message removed");
					//	ignore
					return null;
				} catch (StoreClosedException ex)
				{
					System.err.println("XXXXXXXXXXX STORE CLOSED XXXXXXXX");
					reconnect();
				} catch (FolderClosedException ex)
				{
					System.err.println("XXXXXXXXXXX FOLDER CLOSED XXXXXXXX");
					reOpenFolder(ex.getFolder());
				} catch (MailConnectException ex)
				{
					System.err.println("XXXXXXXXXXX TIMEOUT ISSUE XXXXXXXX");
					//  TODO -- timeout -- not really sure what this does
					LOGGER.info("TIMEOUT -- adapting timeout");
					//timeout = timeout * 2;
				} catch (Exception ex)
				{
					System.err.println("XXXXXXXXXXX OTHER ISSUE XXXXXXXX");
					//	usually a silly error where we did a dbase operation outside a transaction
					LOGGER.info(() -> LoggerUtils.exceptionMessage(ex));
					throw ex;
				}

				LOGGER.log(Level.INFO, "MessageWork::call RE-ATTEMPT {0}", attempts);

				//	TODO -- no idea how to recover from other kinds of exceptions
			}

			throw new StorageException("OPERATION FAILED");
		}
	}
//
//	private class Worker implements Runnable
//	{
//		private final BlockingQueue<? extends Runnable> queue;
//
//		Worker(BlockingQueue<? extends Runnable> queue)
//		{
//			this.queue = queue;
//		}
//
//		@Override
//		public void run()
//		{
//			while (true)
//			{
//				try
//				{
//					Runnable task = queue.take();
//					if (task == Constants.POISON_PILL)
//					{
//						return;
//					}
//					else
//					{
//						ensureMapsLoaded();
//						try
//						{
//							task.run();
//						} catch (RuntimeException ex)
//						{
//							LOGGER.warning(ex.getLocalizedMessage());
//						}
//					}
//				} catch (InterruptedException e)
//				{
//					return;
//				}
//			}
//		}
//	}
}