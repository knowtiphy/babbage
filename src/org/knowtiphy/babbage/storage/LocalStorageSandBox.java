package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * A local storage layer for storing mail messages in a local RDF database (so in the file system of
 * the machine).
 *
 * @author graham
 */
public class LocalStorageSandBox implements IStorage
{
	private static final Logger logger = Logger.getLogger(LocalStorageSandBox.class.getName());

	private static final Runnable POISON_PILL = () -> {
	};
	private static final Map<String, Class<?>> m_Class = new HashMap<>();
	static
	{

		// Map from accountType -> class for the relevant adapter
		m_Class.put(Vocabulary.IMAP_ACCOUNT, IMAPAdapter.class);
		m_Class.put(Vocabulary.CALDAV_ACCOUNT, CALDAVAdapter.class);
		m_Class.put(Vocabulary.CARDDAV_ACCOUNT, CARDDAVAdapter.class);
	}

	private final Dataset messageDatabase;
	private final ListenerManager listenerManager;
	// to interface of adapters
	private final Map<String, IAdapter> m_adapter;
	private final BlockingDeque<Runnable> notificationQ;

	public LocalStorageSandBox(Path databaseLocation, Path accountsFile)
			throws IOException, MessagingException, InterruptedException, NoSuchMethodException, IllegalAccessException,
			InvocationTargetException, InstantiationException
	{
		//	data structures shared between accounts

		messageDatabase = TDB2Factory.connectDataset(databaseLocation.toString());
		listenerManager = new ListenerManager();
		notificationQ = new LinkedBlockingDeque<>();

		//  read accounts -- TODO: this information should all be in a database not a file

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, Files.newInputStream(accountsFile), Lang.TURTLE);

		System.out.println(accountsFile);

		// Theoretically I want add all various subclass of accounts here
		JenaUtils.addSubClasses(model, Vocabulary.IMAP_ACCOUNT, Vocabulary.ACCOUNT);
		JenaUtils.addSubClasses(model, Vocabulary.CALDAV_ACCOUNT, Vocabulary.ACCOUNT);
		JenaUtils.addSubClasses(model, Vocabulary.CARDDAV_ACCOUNT, Vocabulary.ACCOUNT);

		// For each adapter pass them the model and then let them sort it out
		Model accountsModel = ModelFactory.createRDFSModel(model);

		m_adapter = new HashMap<>(100);

		// Testing query
		String getAccountType = "SELECT ?id ?type "
				+ " WHERE {" + "      ?id <" + Vocabulary.RDF_TYPE + "> ?type \n."
				+ "      ?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <" + Vocabulary.ACCOUNT + ">\n."
				+ "       filter(?type != <" + Vocabulary.ACCOUNT + ">)" + "      }";

		ResultSet result = QueryExecutionFactory.create(getAccountType, accountsModel).execSelect();

		while (result.hasNext())
		{
			QuerySolution soln = result.next();
			//System.out.println(soln.varNames());
			//System.out.println(soln.toString());
			Resource name = soln.getResource("id");
			Resource type = soln.getResource("type");

			Class<IAdapter> cls = (Class<IAdapter>) m_Class.get(type.toString());

			IAdapter adapter = cls.getConstructor(String.class, Dataset.class, ListenerManager.class, BlockingDeque.class, Model.class)
					.newInstance(name.toString(), messageDatabase, listenerManager, notificationQ, accountsModel);

			m_adapter.put(adapter.getId(), adapter);

			System.out.println("NAME :: " + name + " TYPE:: " + type);
		}

		System.out.println("AFTER RESULT SET");

		//	thread which notifies listeners of changes
		//noinspection CallToThreadStartDuringObjectConstruction
		new Thread(() -> {
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

	private IAdapter getAccount(String accountId)
	{
		IAdapter adapter = m_adapter.get(accountId);
		assert null != adapter;
		return adapter;
	}

	@Override public ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId)
	{
		return getAccount(accountId).ensureMessageContentLoaded(messageId, folderId);
	}

	// Will call an addListener method in each adapter
	@Override public Map<String, FutureTask<?>> addListener(IStorageListener listener)
			throws StorageException, InterruptedException
	{
		listenerManager.addListener(listener);

		Model accountTriples = ModelFactory.createDefaultModel();
		for (IAdapter adapter : m_adapter.values())
		{
			// TODO: Add triples to each various adapter by giving them the model, and have them pass back the model
			//	     and then that model will notify

			//		But can't I just notify the listeners inside the adapter instead?

			adapter.addListener(accountTriples);
		}

		// Start the synching of the IMAP org.knowtiphy.pinkpigmail.server, adds its work to the front of the Queue
		// but need to put these account of the Queue
		Map<String, FutureTask<?>> accountToFuture = new ConcurrentHashMap<>(100);
		for (IAdapter adapter : m_adapter.values())
		{
			System.out.println("ADAPTER ID :: " + adapter.getId());
			FutureTask<?> futureTask = adapter.getSynchTask();
			System.out.println("FUTURE TASK :: " + futureTask);

			accountToFuture.put(adapter.getId(), futureTask);
			// For each account, spin up a thread that does a sync for it
			new Thread(futureTask).start();
		}

		return accountToFuture;

		//TODO: Snippet of JSON testing for conversion
		/*ByteArrayOutputStream stream = new ByteArrayOutputStream();
		RDFDataMgr.write(stream, mFD, Lang.RDFJSON);

		byte[] data = stream.toByteArray();

		String str = new String(data, StandardCharsets.UTF_8);
		StringReader reader = new StringReader(str);
		System.out.println(str);

		Model modelTest = ModelFactory.createDefaultModel();
		modelTest.read(reader, null, "RDF/JSON");

		JenaUtils.printModel(modelTest, "TEST FROM JSON");*/

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

	@Override public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds)
	{
		return getAccount(accountId).deleteMessages(folderId, messageIds);
	}

	@Override public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
			Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return getAccount(accountId).moveMessagesToJunk(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
			String targetFolderId, boolean delete)
	{
		return getAccount(accountId).copyMessages(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
			boolean flag)
	{
		return getAccount(accountId).markMessagesAsAnswered(messageIds, folderId, flag);
	}

	@Override public Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds,
			boolean flag)
	{
		return getAccount(accountId).markMessagesAsRead(messageIds, folderId, flag);
	}

	@Override public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
			boolean flag)
	{
		return getAccount(accountId).markMessagesAsJunk(messageIds, folderId, flag);
	}

	//	//@Override
	//	public void saveToDrafts(String accountId, String messageId, String draftId) //throws StorageException
	//	{
	////		appendMessages(new Message[]{createMessage(accountId, messageId, true)}, draftId);
	////		rewatch(m_folder.get(draftId));
	//	}

	@Override public void close()
	{
		for (IAdapter adapter : m_adapter.values())
		{
			adapter.close();
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

	@Override public void send(MessageModel model) throws StorageException
	{
		try
		{
			Message message = m_adapter.get(model.getAccountId()).createMessage(model);
			Transport.send(message);
			if (model.getCopyToId() != null)
			{
				m_adapter.get(model.getAccountId()).appendMessages(model.getCopyToId(), new Message[] { message });
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