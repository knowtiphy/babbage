package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.knowtiphy.babbage.storage.IMAP.IMAPAdapter;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * A local storage layer for storing mail messages in a local RDF database (so in the file system of
 * the machine).
 *
 * @author graham
 */
public class LocalStorage implements IStorage
{
	private static final Logger logger = Logger.getLogger(LocalStorage.class.getName());

	private static final Runnable POISON_PILL = () -> {
	};

	private final Dataset messageDatabase;
	private final ListenerManager listenerManager;
	private final Map<String, IMAPAdapter> m_account;
	private final BlockingDeque<Runnable> notificationQ;

	public LocalStorage(Path databaseLocation, Path accountsFile) throws IOException
	{
		//	data structures shared between accounts
		System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXX");

		messageDatabase = TDB2Factory.connectDataset(databaseLocation.toString());
		listenerManager = new ListenerManager();
		notificationQ = new LinkedBlockingDeque<>();

		//  read accounts -- TODO: this information should all be in a database not a file

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, Files.newInputStream(accountsFile), Lang.TURTLE);
		System.out.println(accountsFile);
		JenaUtils.printModel(model, "AAA");

		JenaUtils.addSubClasses(model, Vocabulary.IMAP_ACCOUNT, Vocabulary.ACCOUNT);
		// Getting connection error because it's trying to use Account to connect to the servername
		//JenaUtils.addSubClasses(model, Vocabulary.CALDAV_ACCOUNT, Vocabulary.ACCOUNT);
		//JenaUtils.addSubClasses(model, Vocabulary.CARDDAV_ACCOUNT, Vocabulary.ACCOUNT);

		Model accountsModel = ModelFactory.createRDFSModel(model);
		m_account = new HashMap<>(100);

		ResIterator it = JenaUtils.listSubjectsWithProperty(accountsModel, Vocabulary.RDF_TYPE, Vocabulary.ACCOUNT);

		//ResIterator it1 = JenaUtils.listSubjectsWithProperty(accountsModel, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_ACCOUNT);
		//System.out.println(it1.nextResource().toString());

		System.out.println(it.hasNext());

		while (it.hasNext())
		{
			String name = it.next().asResource().toString();
			System.out.println(name);
			assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
			assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
			assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

			String serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
			String emailAddress = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
			String password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));



			/*String accountId = Vocabulary.E(Vocabulary.IMAP_ACCOUNT, emailAddress);
			Set<String> trustedSenders = new HashSet<>(100);
			JenaUtils.listObjectsOfProperty(accountsModel, name, Vocabulary.HAS_TRUSTED_SENDER)
					.forEachRemaining(x -> trustedSenders.add(x.toString()));
			Set<String> trustedContentProviders = new HashSet<>(100);
			JenaUtils.listObjectsOfProperty(accountsModel, name, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER)
					.forEachRemaining(x -> trustedContentProviders.add(x.toString()));
			m_account.put(accountId, new Account(accountId, messageDatabase, listenerManager,
					notificationQ, serverName, emailAddress, password, trustedSenders, trustedContentProviders));*/
		}

		//	thread which notifies listeners of changes
		//noinspection CallToThreadStartDuringObjectConstruction
		new Thread(() ->
		{
			while (true)
			{
				try
				{
					Runnable task = notificationQ.take();
					if (task == POISON_PILL)
					{
						return;
					}
					else
					{
						try
						{
							task.run();
						} catch (RuntimeException ex)
						{
							logger.warning("Notifier thread failed :: " + ex.getLocalizedMessage());
						}
					}
				} catch (InterruptedException e)
				{
					return;
				}
			}
		}).start();
	}

	// Below two methods copy pasted from Account.java, using as testing and for reference
	private static Resource R(Model model, String name)
	{
		return model.createResource(name);
	}

	private static Property P(ModelCon model, String name)
	{
		return model.createProperty(name);
	}

	private IMAPAdapter getAccount(String accountId)
	{
		IMAPAdapter IMAPAdapter = m_account.get(accountId);
		assert null != IMAPAdapter;
		return IMAPAdapter;
	}

	@Override
	public ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId)
	{
		return getAccount(accountId).ensureMessageContentLoaded(messageId, folderId);
	}

	@Override
	public Map<String, FutureTask<?>> addListener(IStorageListener listener)
	{
		listenerManager.addListener(listener);

		Model accountTriples = ModelFactory.createDefaultModel();
		for (IMAPAdapter IMAPAdapter : m_account.values())
		{
			accountTriples.add(R(accountTriples, IMAPAdapter.getId()), P(accountTriples, Vocabulary.RDF_TYPE), P(accountTriples, Vocabulary.IMAP_ACCOUNT));
			accountTriples.add(R(accountTriples, IMAPAdapter.getId()), P(accountTriples, Vocabulary.HAS_SERVER_NAME), IMAPAdapter
					.getServerName());
			accountTriples.add(R(accountTriples, IMAPAdapter.getId()), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS), IMAPAdapter
					.getEmailAddress());
			accountTriples.add(R(accountTriples, IMAPAdapter.getId()), P(accountTriples, Vocabulary.HAS_PASSWORD), IMAPAdapter
					.getPassword());
			IMAPAdapter.getTrustedSenders().forEach(
					x -> accountTriples.add(R(accountTriples, IMAPAdapter.getId()),
							P(accountTriples, Vocabulary.HAS_TRUSTED_SENDER), x));
			IMAPAdapter.getTrustedContentProviders().forEach(
					x -> accountTriples.add(R(accountTriples, IMAPAdapter.getId()),
							P(accountTriples, Vocabulary.HAS_TRUSTED_CONTENT_PROVIDER), x));
		}

		// Notify the client of the account triples
		TransactionRecorder accountRec = new TransactionRecorder();
		accountRec.addedStatements(accountTriples);
		//listenerManager.notifyChangeListeners(accountRec);

		IReadContext context = getReadContext();
		context.start();

		// So when this added, query the DB and feed those into client the client via notifying the listener
		String constructQueryFD = String
				.format("CONSTRUCT { ?%s <%s> <%s> . " + "?%s <%s> ?%s . " + "?%s <%s> ?%s . " + "?%s <%s> ?%s . "
								+ "?%s <%s> ?%s}\n" + "WHERE \n" + "{\n" + "      ?%s <%s> <%s>.\n" + "      ?%s <%s> ?%s.\n"
								+ "      ?%s <%s> ?%s.\n" + "      ?%s <%s> ?%s.\n" + "      ?%s <%s> ?%s.\n" + "}",
						// START OF CONSTRUCT
						Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT,
						// START OF WHERE
						Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
						Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT);

		Model mFD = QueryExecutionFactory.create(constructQueryFD, context.getModel()).execConstruct();

//		ByteArrayOutputStream stream = new ByteArrayOutputStream();
//		RDFDataMgr.write(stream, mFD, Lang.RDFJSON);
//
//		byte[] data = stream.toByteArray();
//
//		String str = new String(data, StandardCharsets.UTF_8);
//		StringReader reader = new StringReader(str);
//		//System.out.println(string);
//
//		Model modelTest = ModelFactory.createDefaultModel();
//		modelTest.read(reader, null, "RDF/JSON");

		//JenaUtils.printModel(modelTest, "TEST FROM JSON");

		TransactionRecorder rec = new TransactionRecorder();
		rec.addedStatements(mFD);
		//listenerManager.notifyChangeListeners(rec);

		String constructQueryMH = String.format(
				"CONSTRUCT { ?%s <%s> ?%s . ?%s <%s> <%s> . ?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s . "
						+ "?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s . ?%s <%s> ?%s }\n"
						+ "WHERE \n"
						+ "{\n"
						+ "      ?%s <%s> ?%s.\n"
						+ "      ?%s  <%s> <%s>.\n"
						+ "      ?%s  <%s> ?%s.\n"
						+ "      ?%s  <%s> ?%s.\n"
						+ "      ?%s  <%s> ?%s.\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "}",
				// START OF CONSTRUCT
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
				Vars.VAR_MESSAGE_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC,
				// START OF WHERE
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
				Vars.VAR_MESSAGE_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC);

		Model mMH = QueryExecutionFactory.create(constructQueryMH, context.getModel()).execConstruct();
		TransactionRecorder recMH = new TransactionRecorder();
		recMH.addedStatements(mMH);
		context.end();

		//listenerManager.notifyChangeListeners(recMH);

		// Start the synching of the IMAP org.knowtiphy.pinkpigmail.server, adds its work to the front of the Queue
		// but need to put these account of the Queue
		Map<String, FutureTask<?>> accountToFuture = new ConcurrentHashMap<>(100);
		for (IMAPAdapter IMAPAdapter : m_account.values())
		{
			FutureTask<?> futureTask = IMAPAdapter.getSynchTask();
			accountToFuture.put(IMAPAdapter.getId(), futureTask);
			// For each account, spin up a thread that does a sync for it
			new Thread(futureTask).start();
		}

		return accountToFuture;
	}

//	//@Override
//	public String getDraftMessageId(String accountId)
//	{
//		return null;//m_account.get(accountId).getSendId();
//	}
//
//	//@Override
//	public Future<?> appendMessages(String accountId, String folderId, Collection<String> messageIds) //throws StorageException
//	{
//		return null; //getAccount(accountId).appendMessages(folderId, org.knowtiphy.pinkpigmail.messages);
//	}

	@Override
	public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds)
	{
		return getAccount(accountId).deleteMessages(folderId, messageIds);
	}

	@Override
	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return getAccount(accountId).moveMessagesToJunk(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return getAccount(accountId).copyMessages(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds, boolean flag)
	{
		return getAccount(accountId).markMessagesAsAnswered(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds, boolean flag)
	{
		return getAccount(accountId).markMessagesAsRead(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds, boolean flag)
	{
		return getAccount(accountId).markMessagesAsJunk(messageIds, folderId, flag);
	}

//	//@Override
//	public void saveToDrafts(String accountId, String messageId, String draftId) //throws StorageException
//	{
////		appendMessages(new Message[]{createMessage(accountId, messageId, true)}, draftId);
////		rewatch(m_folder.get(draftId));
//	}

	@Override
	public void close()
	{
		for (IMAPAdapter IMAPAdapter : m_account.values())
		{
			IMAPAdapter.close();
		}
		notificationQ.add(POISON_PILL);
		messageDatabase.close();
	}

	//  private code starts
	//
//	private Account connect(Model model, String name) throws IOException, MessagingException, InterruptedException
//	{
//		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
//		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
//		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));
//
//		String serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
//		String emailAddress = JenaUtils
//				.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
//		String password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));
//		Account account = new Account(messageDatabase, listenerManager, notificationQ,
//				serverName, emailAddress, password);
//
//		account.connect();
//		return account;
//	}

	@Override
	public void send(MessageModel model) throws StorageException
	{
		try
		{
			Message message = m_account.get(model.getAccountId()).createMessage(model);
			Transport.send(message);
			if (model.getCopyToId() != null)
			{
				m_account.get(model.getAccountId()).appendMessages(model.getCopyToId(), new Message[]{message});
			}
		} catch (MessagingException | IOException ex)
		{
			throw new StorageException(ex);
		}
//		try
//		{
//			Message message = createMessage(accountId, messageId, false);
//			Transport.send(message);
//			if (sendId != null)
//			{
//				appendMessages(new Message[]{message}, sendId);
//			}
//
//			WriteContext contextw = getWriteContext();
//			contextw.startTransaction();
//			try
//			{
//				//  TODO -- this is wrong, since I don't think unstore unstores everything it needs to
//				DStore.unstoreDraft(contextw.getModel(), messageId);
//				contextw.commit();
//			} finally
//			{
//				contextw.endTransaction();
//			}
//		} catch (MessagingException ex)
//		{
//			throw new StorageException(ex);
//		}
	}
}