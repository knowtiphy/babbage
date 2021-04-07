package org.knowtiphy.babbage.storage.IMAP;

import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IMAPStore;
import com.sun.mail.imap.IdleManager;
import com.sun.mail.util.MailConnectException;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.EventSetBuilder;
import org.knowtiphy.babbage.storage.IAdapter;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.LocalStorage;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.babbage.storage.exceptions.StorageException;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.PriorityExecutor;
import org.knowtiphy.utils.TriConsumer;
import org.knowtiphy.utils.Triple;

import javax.mail.FetchProfile;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.FolderClosedException;
import javax.mail.Message;
import javax.mail.MessageRemovedException;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.StoreClosedException;
import javax.mail.Transport;
import javax.mail.UIDFolder;
import javax.mail.URLName;
import javax.mail.event.MessageChangedEvent;
import javax.mail.event.MessageChangedListener;
import javax.mail.event.MessageCountAdapter;
import javax.mail.event.MessageCountEvent;
import javax.mail.search.FlagTerm;
import javax.mail.search.SearchTerm;
import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static org.knowtiphy.utils.JenaUtils.P;
import static org.knowtiphy.utils.JenaUtils.R;

/**
 * @author graham
 */
public class IMAPAdapter extends BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(IMAPAdapter.class.getName());

	protected final String imapServer;
	protected final String smtpServer;
	protected final String emailAddress;
	protected final String password;

	//	these seem kina of pointless since all we do with trust senders and providers
	//	is store them in the database
	private final Collection<String> trustedSenders, trustedContentProviders;
	private String nickName;

	//	TODO -- can we get rid of these?
	//	map from sub-type of IMAPFolder to folder ID for special folders
	final Map<String, String> specialType2ID = new HashMap<>();

	//	maps from folder id to sub-type of IMAPFolder for special folders
	final Map<String, String> specialId2Type = new HashMap<>();

	private final ExecutorService workService;
	private final ScheduledExecutorService pingService;
	//	these are really the same thing, but having two allows fetching the content of a message immediately, even
	//	if the load ahead service is completely busy.
	private final ExecutorService contentService;
	private final ExecutorService loadAheadService;

	private Store store;
	private IdleManager idleManager;
	protected Properties props;
	private AtomicBoolean closing = new AtomicBoolean(false);

	public IMAPAdapter(String name, String type, Dataset messageDatabase,
					   ListenerManager listenerManager, BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(type, messageDatabase, listenerManager, notificationQ);

		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_IMAP_SERVER);
		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_SMTP_SERVER);
		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_EMAIL_ADDRESS);
		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_PASSWORD);

		imapServer = JenaUtils.getS(model, name, Vocabulary.HAS_IMAP_SERVER);
		smtpServer = JenaUtils.getS(model, name, Vocabulary.HAS_SMTP_SERVER);
		emailAddress = JenaUtils.getS(model, name, Vocabulary.HAS_EMAIL_ADDRESS);
		password = JenaUtils.getS(model, name, Vocabulary.HAS_PASSWORD);
		id = Vocabulary.E(Vocabulary.IMAP_ACCOUNT, emailAddress);

		try
		{
			nickName = JenaUtils.getS(model, name, Vocabulary.HAS_NICK_NAME);
		}
		catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

		trustedSenders = JenaUtils.apply(model, name, Vocabulary.HAS_TRUSTED_SENDER,
				RDFNode::toString, new HashSet<>(100));
		trustedContentProviders = JenaUtils.apply(model, name, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER,
				RDFNode::toString, new HashSet<>(100));

		workService = new PriorityExecutor();
		contentService = Executors.newFixedThreadPool(4);
		loadAheadService = Executors.newFixedThreadPool(4);
		pingService = Executors.newSingleThreadScheduledExecutor();

		operations.put(Vocabulary.SYNC, this::syncFolder);
		operations.put(Vocabulary.SYNC_AHEAD, this::syncAhead);
		operations.put(Vocabulary.MARK_READ, this::markMessagesAsRead);
		operations.put(Vocabulary.DELETE_MESSAGE, this::deleteMessages);
		operations.put(Vocabulary.MARK_JUNK, this::markMessagesAsJunk);
		operations.put(Vocabulary.MARK_ANSWERED, this::markMessagesAsAnswered);
		operations.put(Vocabulary.SEND_MESSAGE, this::sendMessage);
		operations.put(Vocabulary.TRUST_SENDER, this::trustSender);
		operations.put(Vocabulary.TRUST_PROVIDER, this::trustProvider);
	}

	public void initialize(Delta delta) throws MessagingException, IOException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "initialize");
		var eventService = Executors.newCachedThreadPool();
		establistProperties(eventService);
		var session = Session.getInstance(props, null);
		//session.setDebug(true);
		store = session.getStore("imaps");
		store.connect(imapServer, emailAddress, password);
		//	TODO -- one idle manager for all accounts ..
		//	TODO -- check if the provider has IDLE capabilities
		idleManager = new IdleManager(session, eventService);

		initializeFolders();
		addInitialTriples(delta);
		startPinger();

		LOGGER.exiting(this.getClass().getCanonicalName(), "initialize");
	}

	//	add account triples to the message cache
	//	note: it's a cache -- so we can't simply store these triples in the cache permnantly, as
	//	the cache is designed to be deleteable at any point in time.

	private void addInitialTriples(Delta delta)
	{
		delta.bothOP(id, RDF.type.toString(), type);

		specialId2Type.forEach((fid, type) ->
		{
			delta.bothOP(id, Vocabulary.HAS_SPECIAL, fid);
			//	this isn't necessary as addFolder will do this
			//delta.bothOP(fid, RDF.type.toString(), type);
		});

		delta.bothDPN(getId(), Vocabulary.HAS_NICK_NAME, nickName);
		delta.bothDP(getId(), Vocabulary.HAS_EMAIL_ADDRESS, emailAddress);
		delta.bothDP(getId(), Vocabulary.HAS_SERVER_NAME, imapServer);

		trustedSenders.forEach(s -> delta.bothDP(getId(), Vocabulary.HAS_TRUSTED_SENDER, s));
		trustedContentProviders.forEach(s -> delta.bothDP(getId(), Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER, s));
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	public void close(Model model)
	{
		closing.set(true);
		LOGGER.entering(this.getClass().getCanonicalName(), "close");

		LOGGER.log(Level.INFO, "Saving account information");
		IProcedure.doAndIgnore(() -> save(model));

		LOGGER.log(Level.INFO, "Shutting down ping service");
		IProcedure.doAndIgnore(pingService::shutdown);
		IProcedure.doAndIgnore(() -> pingService.awaitTermination(10, TimeUnit.SECONDS));

		LOGGER.log(Level.INFO, "shutting down worker pools");
		IProcedure.doAndIgnore(workService::shutdown);
		IProcedure.doAndIgnore(() -> workService.awaitTermination(10, TimeUnit.SECONDS));
		IProcedure.doAndIgnore(contentService::shutdown);
		IProcedure.doAndIgnore(() -> contentService.awaitTermination(10, TimeUnit.SECONDS));
		IProcedure.doAndIgnore(loadAheadService::shutdown);
		IProcedure.doAndIgnore(() -> loadAheadService.awaitTermination(10, TimeUnit.SECONDS));

		LOGGER.log(Level.INFO, "closing store");
		IProcedure.doAndIgnore(store::close);

		LOGGER.exiting(this.getClass().getCanonicalName(), "close");
	}

	private void save(Model model)
	{
		String name = LocalStorage.nameSource.get();
		JenaUtils.addType(model, name, Vocabulary.IMAP_ACCOUNT);
		JenaUtils.addDP(model, name, Vocabulary.HAS_SERVER_NAME, imapServer);
		JenaUtils.addDP(model, name, Vocabulary.HAS_EMAIL_ADDRESS, emailAddress);
		JenaUtils.addDP(model, name, Vocabulary.HAS_PASSWORD, password);
		trustedContentProviders.forEach(cp ->
				JenaUtils.addDP(model, name, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER, cp));
		trustedSenders.forEach(s -> JenaUtils.addDP(model, name, Vocabulary.HAS_TRUSTED_SENDER, s));

		JenaUtils.printModel(model, "Saved Model");
	}

	//	TODO -- this needs to call initialize since initialize is really just the initial sync
	@Override
	public void sync()
	{
	}

	public Future<?> sync(String fid) throws ExecutionException, InterruptedException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "sync");

		return addEventWork(() -> {
			Folder folder = F(fid);
			watch(folder);

			var delta = new Delta();
			synchronizeFolder(folder, delta);

			var event = new EventSetBuilder();
			var eid = event.newEvent(Vocabulary.FOLDER_SYNCED);
			event.addOP(eid, Vocabulary.HAS_ACCOUNT, id).
					addOP(eid, Vocabulary.HAS_FOLDER, fid);

			LOGGER.exiting(this.getClass().getCanonicalName(), "sync");
			return new Triple<>(folder, delta, event);
		});
	}

	private Future<?> markMessages(String oid, Model operation, TriConsumer<Folder, Message[], Boolean> doIt)
	{
		var fid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_FOLDER).toString();
		var flag = JenaUtils.getB(operation, oid, Vocabulary.HAS_FLAG);
		var mids = getMessageIDs(oid, operation);

		return addWork(new MessageWork(() -> {
			Folder folder = F(fid);
			Message[] messages = Encode.U(folder, mids);
			System.out.println(operation);
			System.out.println(folder);
			System.out.println(Arrays.toString(messages));
			System.out.println(flag);
			doIt.accept(folder, messages, flag);
			return List.of(folder);
		}));
	}

	public Future<?> markMessagesAsAnswered(String oid, Model operation)
	{
		return markMessages(oid, operation,
				(folder, msgs, flag) -> mark(msgs, new Flags(Flags.Flag.ANSWERED), flag));
	}

	private Future<?> markMessagesAsRead(String oid, Model operation)
	{
		return markMessages(oid, operation,
				(folder, msgs, flag) -> mark(msgs, new Flags(Flags.Flag.SEEN), flag));
	}

	public Future<?> markMessagesAsJunk(String oid, Model operation)
	{
		return markMessages(oid, operation,
				(folder, msgs, flag) -> mark(msgs, new Flags(Constants.JUNK_FLAG), flag));
	}

	//  TOOD -- got to go since its just a composition of mark junk and copy org.knowtiphy.pinkpigmail.messages -- merge with move
	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
										boolean delete)
	{
		return addWork(() -> {
			LOGGER.log(Level.INFO, "moveMessagesToJunk : {0}", delete);
			Message[] messages = Encode.U(F(sourceFolderId), messageIds);
			Folder sourceFolder = F(sourceFolderId);
			Folder targetFolder = F(targetFolderId);
			mark(messages, new Flags(Constants.JUNK_FLAG), true);
			sourceFolder.copyMessages(messages, targetFolder);
			if (delete)
			{
				mark(messages, new Flags(Flags.Flag.DELETED), true);
				sourceFolder.expunge();
			}

			return List.of(sourceFolder, targetFolder);
		});
	}

	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								  boolean delete) throws MessagingException
	{
		Folder source = F(sourceFolderId);
		Folder target = F(targetFolderId);

		System.out.println("COPY MESSAGES");
		System.out.println(messageIds);

		return addWork(new MessageWork(() -> {
			//  TODO -- this is wrong, since can have both source and target close/fail
			Message[] messages = Encode.U(F(sourceFolderId), messageIds);
			System.out.println("COPY MESSAGES MESSAGES");
			System.out.println(Arrays.toString(messages));
			source.copyMessages(messages, target);
			//  is this even necessary, or is the semantics of copy already a delete in the original folder?
			if (delete)
			{
				mark(messages, new Flags(Flags.Flag.DELETED), true);
				source.expunge();
			}
			return List.of(source);
		}));
	}

	private Future<?> deleteMessages(String oid, Model operation)
	{
		System.out.println("IN SERVER DELETE");
		var fid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_FOLDER).toString();
		var mids = getMessageIDs(oid, operation);
		System.out.println("DONE SERVER DELETE");

		return addWork(new MessageWork(() -> {
			var folder = F(fid);
			Message[] messages = Encode.U(folder, mids);
			System.out.println("MARKING MESSSAGES ARE DELETED " + mids);
			mark(messages, new Flags(Flags.Flag.DELETED), true);
			folder.expunge();
			return List.of(folder);
		}));


	}

	private Future<?> sendMessage(String oid, Model operation)
	{
		try
		{
			var msg = CreateMessage.createMessage(this, oid, operation);
			System.out.println(msg);
			Transport.send(msg);
		}
		catch (MessagingException | IOException e)
		{
			e.printStackTrace();
		}

		return new FutureTask<>(() -> true);
	}

	private Future<?> trustSender(String oid, Model operation)
	{
		System.out.println("TRUST SENDER ");
		JenaUtils.printModel(operation);
		Delta delta = new Delta();
		JenaUtils.apply(operation, oid, Vocabulary.HAS_TRUSTED_SENDER,
				s -> delta.addDP(id, Vocabulary.HAS_TRUSTED_SENDER, s.toString()));
		System.out.println(delta);
		apply(delta);
		//	TODO -- fire off an event
		return new FutureTask<>(() -> true);
	}

	private Future<?> trustProvider(String oid, Model operation)
	{
		System.out.println("TRUST PROVIDER ");
		JenaUtils.printModel(operation);
		Delta delta = new Delta();
		JenaUtils.apply(operation, oid, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER,
				s -> delta.addDP(id, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER, s.toString()));
		System.out.println(delta);
		apply(delta);
		//	TODO -- fire off an event
		return new FutureTask<>(() -> true);
	}

	public Future<?> appendMessages(String fid, Message[] messages)
	{
		return addWork(new MessageWork(() -> {
			var folder = F(fid);
			folder.appendMessages(messages);
			return List.of(folder);
		}));
	}

	private Future<?> syncFolder(String oid, Model operation)
	{
		var fid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_FOLDER).toString();
		var mid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_MESSAGE).toString();
		//System.out.println("syncOp = " + mid);

		return contentService.submit(new MessageWork(() -> load(fid, List.of(mid))));
	}

	private Future<?> syncAhead(String oid, Model operation)
	{
		var fid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_FOLDER).toString();
		var mids = getMessageIDs(oid, operation);
		//System.out.println("syncOp = " + mids);

		return loadAheadService.submit(() -> load(fid, mids));
	}

	@Override
	public String toString()
	{
		return "IMAPAdapter{" +
				"id='" + id + '\'' +
				", type='" + type + '\'' +
				", emailAddress='" + emailAddress + '\'' +
				'}';
	}

	//	private methods start here

	private void establistProperties(ExecutorService eventService)
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "establistProperties");

		props = new Properties();

		//	TODO -- all these need to be in RDF
		props.put("mail.store.protocol", "imaps");
		props.put("mail.imaps.host", imapServer);
		props.put("mail.imaps.usesocketchannels", "true");
		props.put("mail.imaps.peek", "true");
		//	this should be set to true/false depending on whether the store has the
		//	COMPRESS capability, but we have to create the session with properties
		//	before we get a store!?
		//incomingProperties.put("mail.imaps.compress.enable", "true");
		props.put("mail.imaps.connectionpoolsize", "20");
		//	TODO -- what is a sane number for this?
		props.put("mail.imaps.fetchsize", "3000000");
		props.setProperty("mail.imaps.connectiontimeout", "2000");
		props.setProperty("mail.imaps.timeout", "2000");
		// incoming.setProperty("mail.imaps.port", "993");

		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.host", smtpServer);
		props.put("mail.smtp.ssl.enable", "true");
		//	props.put("mail.smtp.port", "465");
		//	do I need this?
		props.put("mail.smtp.auth", "true");

		props.put("mail.event.scope", "session"); // or "application"
		props.put("mail.event.executor", eventService);

		//	we need system properties to pick up command line flags
		props.putAll(System.getProperties());

		LOGGER.exiting(this.getClass().getCanonicalName(), "establishIncomingProperties");
	}

	private Folder open(Folder folder) throws MessagingException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "openFolder");
		folder.open(Folder.READ_WRITE);
		LOGGER.exiting(this.getClass().getCanonicalName(), "openFolder");
		return folder;
	}

	private Folder F(String id) throws MessagingException
	{
		Folder f = store.getFolder(new URLName(id).getFile());
		//	this assertion will be crap later but for the moment it will do
		assert f != null;
		assert f.exists();
		return open(f);
	}

	private Folder[] getFolders() throws MessagingException
	{
		return store.getDefaultFolder().list();
	}

	private void fetchMessages(Folder folder, Message[] msgs) throws MessagingException
	{
		//	TODO -- this possibly needs to be tuned a bit
		FetchProfile fp = new FetchProfile();
		fp.add(FetchProfile.Item.CONTENT_INFO);
		fp.add(FetchProfile.Item.ENVELOPE);
		fp.add(FetchProfile.Item.FLAGS);
		fp.add(IMAPFolder.FetchProfileItem.MESSAGE);
		fp.add(UIDFolder.FetchProfileItem.UID);
		fp.add(IMAPFolder.FetchProfileItem.HEADERS);

		folder.fetch(msgs, fp);
	}

	private List<Folder> load(String folderId, Collection<String> messageIds)
	{
		//System.out.println("LOAD " + folderId + " :: " + messageIds);

		//	TODO -- not sure this is correct -- what happens if we get a delete/add between checking its stored and
		//	the fetch of the message -- perhaps the single threadedness helps us?
		//	definitely incorrect? Need to hold a write lock for the whole thing

		//	System.out.println("load WORKER : calcing need to fetch");
		try
		{
			List<String> needToFetch = new ArrayList<>();
			for (String messageId : messageIds)
			{
				boolean stored = query(() -> cache.getDefaultModel().
						listObjectsOfProperty(R(cache.getDefaultModel(), messageId),
								P(cache.getDefaultModel(), Vocabulary.HAS_CONTENT)).hasNext());
				if (!stored)
				{
					needToFetch.add(messageId);
				}

				if (!needToFetch.isEmpty())
				{
					//System.out.println("NEED TO FETCH = " + needToFetch);
					Folder folder = F(folderId);
					Message[] msgs = Encode.U(folder, needToFetch);
					fetchMessages(folder, msgs);

					//	get all the data first since we don't want to hold a write lock if the IMAP fetching stalls
					//System.out.println("FETCHED " + (System.currentTimeMillis() - t) + " = " + needToFetch);
					var delta = new Delta();
					//List<MessageContent> contents = new LinkedList<>();
					for (Message message : msgs)
					{
						var mid = Encode.encode(message);
						DStore.addMessageContent(delta, message, mid);
					}
					//System.out.println("CONTENT LOADED " + (System.currentTimeMillis() - t) + " = " + needToFetch);

//					var delta = new Delta();
//					contents.forEach(content -> DStore.addMessageContent(delta, content));
					apply(delta);

					//System.out.println("load WORKER DONE : " + needToFetch);
					return List.of(folder);
				}
			}
		}
		catch (Exception ex)
		{
			System.out.println("load WORKER : FAILED");
			ex.printStackTrace();
		}

		return new LinkedList<>();
	}

	private void startPinger() throws MessagingException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "::startPinger");
		var inbox = specialType2ID.get(Vocabulary.INBOX_FOLDER);
		assert inbox != null;
		pingService.scheduleAtFixedRate(new Ping(this, F(inbox)),
				Constants.PING_FREQUENCY, Constants.PING_FREQUENCY, TimeUnit.MINUTES);
		LOGGER.exiting(this.getClass().getCanonicalName(), "startPinger");
	}

	private void setSpecialFolder(String type, Folder folder, Pattern[] alternatives) throws MessagingException//, Consumer<String> setFolder) throws MessagingException
	{
		String special = specialType2ID.get(type);
		if (special == null)
		{
			for (Pattern p : alternatives)
			{
				if (p.matcher(folder.getName()).matches())
				{
					specialType2ID.put(type, Encode.encode(folder));
					return;
				}
			}
		}
	}

	private void initializeFolders() throws MessagingException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "initializeFolders");

		//	compute special folders
		specialType2ID.put(Vocabulary.INBOX_FOLDER, Encode.encode(F("INBOX")));
		if (((IMAPStore) store).hasCapability("SPECIAL-USE"))
		{
			for (Folder folder : getFolders())
			{
				LOGGER.log(Level.CONFIG, "{0} :: Folder = {1}, attributes = {2}",
						new String[]{emailAddress, folder.getName(), Arrays.toString(((IMAPFolder) folder).getAttributes())});
				for (String attr : ((IMAPFolder) folder).getAttributes())
				{
					if (Constants.ARCHIVES_ATTRIBUTE.matcher(attr).matches())
					{
						specialType2ID.put(Vocabulary.ARCHIVE_FOLDER, Encode.encode(folder));
					}
					if (Constants.DRAFTS_ATTRIBUTE.matcher(attr).matches())
					{
						specialType2ID.put(Vocabulary.DRAFTS_FOLDER, Encode.encode(folder));
					}
					if (Constants.JUNK_ATTRIBUTE.matcher(attr).matches())
					{
						specialType2ID.put(Vocabulary.JUNK_FOLDER, Encode.encode(folder));
					}
					if (Constants.SENT_ATTRIBUTE.matcher(attr).matches())
					{
						specialType2ID.put(Vocabulary.SENT_FOLDER, Encode.encode(folder));
					}
					if (Constants.TRASH_ATTRIBUTE.matcher(attr).matches())
					{
						specialType2ID.put(Vocabulary.TRASH_FOLDER, Encode.encode(folder));
					}
				}
			}
		}

		//	we run this code to cover two scenarios:
		//	a) we didn't have the SPECIAL-USE capability
		//	b) we did have SPECIAL-USE but did not detect all the special folders (some IMAP servers
		//	don't have every special folder marked as such)

		for (Folder folder : getFolders())
		{
			setSpecialFolder(Vocabulary.ARCHIVE_FOLDER, folder, Constants.ARCHIVE_PATTERNS);//, f -> archive = f);
			setSpecialFolder(Vocabulary.DRAFTS_FOLDER, folder, Constants.DRAFT_PATTERNS);//, f -> drafts = f);
			setSpecialFolder(Vocabulary.JUNK_FOLDER, folder, Constants.JUNK_PATTERNS);//, f -> junk = f);
			setSpecialFolder(Vocabulary.SENT_FOLDER, folder, Constants.SENT_PATTERNS);//, f -> sent = f);
			setSpecialFolder(Vocabulary.TRASH_FOLDER, folder, Constants.TRASH_PATTERNS);//, f -> trash = f);
		}

		//	TODO -- two maps, or even any maps, IS A BUT CLUMSY
		specialType2ID.forEach((type, id) -> specialId2Type.put(id, type));

		//	TODO -- use the new diff command in QueryHelper
		var storedFIDs = query(() -> DFetch.folderIDs(cache, getId()));
		var imapFIDS = new HashSet<String>();
		for (Folder folder : getFolders())
		{
			imapFIDS.add(Encode.encode(folder));
		}

		//	need to add folders in imap but not stored
		var addUID = new HashSet<String>();
		imapFIDS.forEach(mid -> {
			if (!storedFIDs.contains(mid))
			{
				addUID.add(mid);
			}
		});

		//	need to delete folders in stored but not in imap
		var removeUID = new HashSet<String>();
		storedFIDs.forEach(mid ->
		{
			if (!imapFIDS.contains(mid))
			{
				removeUID.add(mid);
			}
		});

		Delta delta = new Delta();

		for (Folder folder : getFolders())
		{
			var fid = Encode.encode(folder);
			if (addUID.contains(fid))
			{
				DStore.addFolder(delta, this, folder);
			}
			if (removeUID.contains(fid))
			{
				DStore.deleteFolder(cache, delta, fid);
			}
		}

		//	update the database
		apply(delta);

		LOGGER.log(Level.CONFIG, "{0} :: Special Folders :: {1}", new String[]{emailAddress, specialType2ID.toString()});

		LOGGER.exiting(this.getClass().getCanonicalName(), "initializeFolders");
	}

	private void watch(Folder folder) throws MessagingException
	{
		assert folder != null;
		LOGGER.entering(this.getClass().getCanonicalName(), folder.getName() + "::watch");
		folder.addMessageCountListener(new WatchCountChanges());
		folder.addMessageChangedListener(new WatchMessageChanges());
		//System.out.println("WATCHING " + folder + " " + folder.isOpen());
		idleManager.watch(folder);
		LOGGER.exiting(this.getClass().getCanonicalName(), folder.getName() + "::watch");
	}

	void rewatch(Folder folder) throws MessagingException
	{
		LOGGER.entering(this.getClass().getCanonicalName(), folder.getName() + "::rewatch");
		//System.out.println("REWATCHING " + folder + " " + folder.isOpen());
		idleManager.watch(folder);
		LOGGER.exiting(this.getClass().getCanonicalName(), folder.getName() + "::rewatch");
	}

	//	synch methods

	private void synchronizeFolder(Folder folder, Delta delta) throws Exception
	{
		LOGGER.entering(this.getClass().getCanonicalName(), folder.getName() + "::synchronizeFolder");

		String fid = Encode.encode(folder);

		//	check if we have already stored this folder
		var isStored = query(() -> DFetch.hasFolder(cache.getDefaultModel(), fid));

		//	if we have stored the folder delete its folder counts as they may have changed
		if (isStored)
		{
			//  TODO -- should really check that the validity hasn't changed
			query(() -> DStore.deleteFolderCounts(cache, delta, fid));
		}
		//	add a new folder
		else
		{
			DStore.addFolder(delta, this, folder);
		}

		//	set the new folder counts
		DStore.addFolderCounts(delta, folder, fid);

		//  get the stored message IDs and the IMAP message ids

		var stored = getStoredMessageIDs(fid);
		var imap = getIMAPMessageIDs(folder);

		//	need to add messages in imap but not stored
		var addUID = new HashSet<String>();
		imap.forEach(mid -> {
			if (!stored.contains(mid))
			{
				addUID.add(mid);
			}
		});

		//	need to delete messages in stored but not in imap
		var removeUID = new HashSet<String>();
		stored.forEach(mid ->
		{
			if (!imap.contains(mid))
			{
				removeUID.add(mid);
			}
		});

		removeUID.forEach(message -> query(() -> DStore.deleteMessage(cache, delta, fid, message)));
		addUID.forEach(message -> DStore.addMessage(delta, fid, message));

		//  get message headers for message ids that we don't have headers for
		var withHeaders = query(() -> DFetch.messageIds(cache, DFetch.messageUIDsWithHeaders(id, fid)));

		//	we need to fetch headers for messages that are stored but don't have headers, or new
		//	messages that are being added

		var fetchHeaders = new HashSet<>(stored);
		fetchHeaders.removeAll(withHeaders);
		fetchHeaders.addAll(addUID);

		if (!fetchHeaders.isEmpty())
		{
			Message[] msgs = Encode.U(folder, addUID);
			FetchProfile fp = new FetchProfile();
			//	TODO: headers vs envelope?
			fp.add(FetchProfile.Item.ENVELOPE);
			fp.add(FetchProfile.Item.FLAGS);
			fp.add(IMAPFolder.FetchProfileItem.HEADERS);
			fp.add(UIDFolder.FetchProfileItem.UID);
			folder.fetch(msgs, fp);

			for (Message msg : msgs)
			{
				String mid = Encode.encode(msg);
				if (stored.contains(mid))
				{
					DStore.deleteMessageFlags(cache, delta, mid);
				}
				DStore.addMessageFlags(delta, msg, mid);
				DStore.addMessageHeaders(delta, msg, mid);
			}
		}

		//	TODO -- what about stored messages whose headers have changed?

		LOGGER.exiting(this.getClass().getCanonicalName(), folder.getName() + "::synchronizeFolder");
	}

	//	TODO --- ?? Note: setFlags does not fail if one of the org.knowtiphy.pinkpigmail.messages is deleted
	private static void mark(Message[] messages, Flags flags, boolean value) throws MessagingException
	{
		//	can be 0 if all the messages were previously deleted
		if (messages.length > 0)
		{
			System.out.println("mark");
			System.out.println(Arrays.toString(messages));
			System.out.println(flags);
			messages[0].getFolder().setFlags(messages, flags, value);
		}
	}

	//	work stuff

	protected <T> Future<T> addWork(Callable<T> operation)
	{
		return workService.submit(() -> {
			try
			{
				return operation.call();
			}
			catch (Throwable ex)
			{
				//	TODO -- make an event and notify about it
				ex.printStackTrace(System.out);
				LOGGER.warning(ex.getLocalizedMessage());
				return null;
			}
		});
	}

	protected Future<Triple<Folder, Delta, EventSetBuilder>> addEventWork(Callable<Triple<Folder, Delta, EventSetBuilder>> operation)
	{
		//	TODO -- if event work fails we need to tell the client and let them resync
		return addWork(() -> {
			var t = operation.call();
			rewatch(t.getFirst());
			applyAndNotify(t.getSecond(), t.getThird());
			return t;
		});
	}

	private Collection<String> getMessageIDs(String from, Model model)
	{
		var mids = new LinkedList<String>();
		JenaUtils.listObjectsOfProperty(model, from, Vocabulary.HAS_MESSAGE).
				forEachRemaining(mid -> mids.add(mid.toString()));
		return mids;
	}

	private Set<String> getStoredMessageIDs(String fid)
	{
		return query(() -> DFetch.messageIds(cache, DFetch.messageUIDs(fid)));
	}

	private Set<String> getIMAPMessageIDs(Folder folder) throws MessagingException
	{
		SearchTerm st = new FlagTerm(new Flags(Flags.Flag.DELETED), false);
		Message[] msgs = folder.search(st);
		FetchProfile fp = new FetchProfile();
		fp.add(UIDFolder.FetchProfileItem.UID);
		folder.fetch(msgs, fp);

		Set<String> folderMessages = new HashSet<>();
		for (Message msg : msgs)
		{
			folderMessages.add(Encode.encode(msg));
		}

		return folderMessages;
	}

	//	TODO -- this needs to be upgraded to a list of folders to rewatch and hence no folders
	private class MessageWork implements Callable<Collection<Folder>>
	{
		private final Callable<Collection<Folder>> work;

		MessageWork(Callable<Collection<Folder>> work)
		{
			this.work = work;
		}

		@Override
		public Collection<Folder> call() throws Exception
		{
			for (int attempts = 0; attempts < Constants.NUM_ATTEMPTS; attempts++)
			{
				try
				{
					//	System.out.println("DOING MESSAGE WORK");
					Collection<Folder> folders = work.call();
					for (Folder f : folders)
					{
						//	what do we do if rewatch fails?
						rewatch(f);
					}
					return folders;
				}
				catch (MessageRemovedException ex)
				{
					LOGGER.info("----- MessageWork :: message removed -----");
					//	ignore
					return null;
				}
				catch (StoreClosedException ex)
				{
					LOGGER.info("----- MessageWork :: store closed -----");
					//reconnect();
				}
				catch (FolderClosedException ex)
				{
					LOGGER.info("----- MessageWork :: folder closed -----");
					//recoverFromClosedFolder(ex.getFolder());
				}
				catch (MailConnectException ex)
				{
					//  TODO -- timeout -- not really sure what this does
					LOGGER.info("----- MessageWork :: MailConnectException -----");
					//timeout = timeout * 2;
				}
				catch (Exception ex)
				{
					//	usually a silly error where we did a dbase operation outside a transaction
					LOGGER.info("----- MessageWork :: Other Issue -----");
					LOGGER.info(LoggerUtils.exceptionMessage(ex));
					throw ex;
				}

				LOGGER.log(Level.INFO, "MessageWork::call RE-ATTEMPT {0}", attempts);

				//	TODO -- no idea how to recover from other kinds of exceptions
			}

			throw new StorageException("OPERATION FAILED");
		}
	}

	// handle incoming messages from the IMAP server
	private class WatchMessageChanges implements MessageChangedListener
	{
		private boolean isDeleted(Message message) throws MessagingException
		{
			try
			{
				//	do anything that can cause a MessageRemovedException
				message.isSet(Flags.Flag.SEEN);
				return false;
			}
			catch (MessageRemovedException ex)
			{
				return true;
			}
		}

		@Override
		public void messageChanged(MessageChangedEvent messageChangedEvent)
		{
			LOGGER.log(Level.INFO, "HAVE A MESSAGE CHANGED {0}", messageChangedEvent);

			var folder = (Folder) messageChangedEvent.getSource();
			var message = messageChangedEvent.getMessage();

			var delta = new Delta();
			var event = new EventSetBuilder();
			var eid = event.newEvent(Vocabulary.MESSAGE_FLAGS_CHANGED);
			event.addOP(eid, Vocabulary.HAS_ACCOUNT, id);

			addEventWork(() ->
			{
				var fid = Encode.encode(folder);
				if (messageChangedEvent.getMessageChangeType() == MessageChangedEvent.FLAGS_CHANGED)
				{
					event.addOP(eid, Vocabulary.HAS_FOLDER, fid);

					//	message flag changing can indicate that folder counts have changed
					query(() -> DStore.deleteFolderCounts(cache, delta, fid));
					DStore.addFolderCounts(delta, folder, fid);

					//	as long as the change is not a delete, adjust the flags (deletes are handled
					//	in WatchCountChanges.messagesRemoved
					if (!isDeleted(message))
					{
						String mid = Encode.encode(message);
						query(() -> DStore.deleteMessageFlags(cache, delta, mid));
						DStore.addMessageFlags(delta, message, mid);
						event.addOP(eid, Vocabulary.HAS_MESSAGE, mid);
					}
				}
				else
				{
					//	TODO -- have to handle this case
					LOGGER.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX---WTF -- the envelope changed????");
				}

				return new Triple<>(folder, delta, event);
			});
		}
	}

	private class WatchCountChanges extends MessageCountAdapter
	{
		@Override
		public void messagesRemoved(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "WatchCountChanges::messagesRemoved {0}", e.getMessages().length);

			//	TODO-- ASSUMING this folder is the same for all messages?
			var folder = (Folder) e.getSource();

			var delta = new Delta();
			var event = new EventSetBuilder();

			var eid = event.newEvent(Vocabulary.MESSAGE_DELETED);
			event.addOP(eid, Vocabulary.HAS_ACCOUNT, id);

			addEventWork(() ->
			{
				var fid = Encode.encode(folder);
				event.addOP(eid, Vocabulary.HAS_FOLDER, fid);

				DStore.addFolderCounts(delta, folder, fid);
				query(() -> DStore.deleteFolderCounts(cache, delta, fid));

				//	we cannot loop through the messages to get the ids of the deleted messages
				//	so we have to compute folder's stored - imap messages

				var stored = getStoredMessageIDs(fid);
				var imap = getIMAPMessageIDs(folder);
				var deleted = new HashSet<String>();

				//  we need to remove messages in the set (stored - folder)
				stored.forEach(mid ->
				{
					if (!imap.contains(mid))
					{
						deleted.add(mid);
					}
				});

				deleted.forEach(mid -> {
					query(() -> DStore.deleteMessage(cache, delta, fid, mid));
					event.addOP(eid, Vocabulary.HAS_MESSAGE, mid);
				});

				return new Triple<>(folder, delta, event);
			});
		}

		@Override
		public void messagesAdded(MessageCountEvent e)
		{
			LOGGER.log(Level.INFO, "messagesAdded {0}", Arrays.toString(e.getMessages()));

			//	TODO-- ASSUMING this  is the same for all messages?
			var folder = (Folder) e.getSource();

			var delta = new Delta();
			var event = new EventSetBuilder();
			var eid = event.newEvent(Vocabulary.MESSAGE_ARRIVED);
			event.addOP(eid, Vocabulary.HAS_ACCOUNT, id);

			addEventWork(() ->
			{
				var fid = Encode.encode(folder);
				event.addOP(eid, Vocabulary.HAS_FOLDER, fid);

				DStore.addFolderCounts(delta, folder, fid);
				query(() -> DStore.deleteFolderCounts(cache, delta, fid));

				for (Message message : e.getMessages())
				{
					fetchMessages(folder, new Message[]{message});
					var mid = Encode.encode(message);
					DStore.addMessage(delta, fid, mid);
					DStore.addMessageFlags(delta, message, mid);
					DStore.addMessageHeaders(delta, message, mid);
					DStore.addMessageContent(delta, message, mid);
					event.addOP(eid, Vocabulary.HAS_MESSAGE, mid);
				}

				return new Triple<>(folder, delta, event);
			});
		}

	}
}

//		System.out.println("CAPABILITES ----------------------- " + nickName);
//		System.out.println(((IMAPStore) store).hasCapability("LIST-EXTENDED"));
//		System.out.println(((IMAPStore) store).hasCapability("SPECIAL-USE"));
//		System.out.println(((IMAPStore) store).hasCapability("IDLE"));

//		specialId2Type.forEach((id, type) -> {
//			JenaUtils.addOP(model, getId(), Vocabulary.HAS_SPECIAL, id);
//			JenaUtils.addType(model, id, type);
//		});


//	@Override
//	public Model getSpecialFolders()
//	{
//		Model folders = ModelFactory.createDefaultModel();
//		specialId2Type.forEach((id, type) -> JenaUtils.addType(folders, id, type));
//		return folders;
//	}

//protected void reStartPingThread() throws MessagingException
//{
//	String inbox = specialFolders.get(Vocabulary.INBOX_FOLDER);
//	if (inbox != null && !F(inbox).isOpen())
//	{
//		//reOpenFolder(inbox);
//		startPingThread();
//	}
//}


//	reconnect, reopen folders, etc methods

//	void reconnect() //throws MessagingException
//	{
//		//	TODO -- have to work out how to do this -- leave till later
//		//	the docs says its fatal and all messaging objects are now toast -- not sure if that means messages or
//		//	folders as well
//		LOGGER.log(Level.INFO, "RECONNECTING WITH NO RECOVERY :: {0}", emailAddress);
//		//		if (store.isConnected())
//		//		{
//		//			store.close();
//		//		}
//		//		store.connect(serverName, emailAddress, password);
//		//		logger.info("END RECONNECTING");
//		//		for (Folder folder : folders)
//		//		{
//		//			logger.info("REOPENING :: " + folder.getName());
//		//			if (folder.isOpen())
//		//			{
//		//				folder.close();
//		//			}
//		//
//		//			folder.open(javax.mail.Folder.READ_WRITE);
//		//			logger.info("END REOPENING");
//		//		}
//	}
//	protected <T> Future<T> addPriorityWork(Callable<T> operation)
//	{
//		return workService.submit(new PriorityWork<>(operation, Constants.SYNCH_PRIORITY));
//	}
//	recover from a closed folder

//	void recoverFromClosedFolder(Folder folder) throws Exception
//	{
//		LOGGER.entering(this.getClass().getCanonicalName(), emailAddress + "::recoverFromClosedFolder");
//
//		open(folder);
//		//	synchMessageIdsAndHeaders(folder);
//
//		LOGGER.exiting(this.getClass().getCanonicalName(), emailAddress + "::recoverFromClosedFolder");
//	}
//

//		doAndIgnore(pingService::shutdown);
//		doAndIgnore(() -> pingService.awaitTermination(10, TimeUnit.SECONDS));
//			pingService.scheduleAtFixedRate(new Ping(this, inbox),
//					Constants.PING_FREQUENCY, Constants.PING_FREQUENCY, TimeUnit.MINUTES);
//pingService = Executors.newSingleThreadScheduledExecutor();
////		if (pingThread != null)
//		{
//				pingThread.interrupt();
//				}
//private Thread pingThread;
//			pingThread = new Thread(new PingThread(this, F(inbox), Constants.FREQUENCY));
//			pingThread.setDaemon(true);
//			pingThread.start();
