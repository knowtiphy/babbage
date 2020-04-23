package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.knowtiphy.babbage.Babbage;
import org.knowtiphy.babbage.storage.CALDAV.CALDAVAdapter;
import org.knowtiphy.babbage.storage.CARDDAV.CARDDAVAdapter;
import org.knowtiphy.babbage.storage.IMAP.IMAPAdapter;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.OS;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
public class LocalStorageSandBox implements IStorage
{
	private static final Logger logger = Logger.getLogger(LocalStorageSandBox.class.getName());

	private static final String CACHE = "cache";
	private static final String ACCOUNTS_FILE = "accounts.ttl";

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

	public LocalStorageSandBox() throws Exception
	{
		//	data structures shared between accounts

		Path databaseLocation = Paths.get(OS.getDataDir(Babbage.class).toString(), CACHE);
		Files.createDirectories(databaseLocation);

		messageDatabase = TDB2Factory.connectDataset(databaseLocation.toString());
		listenerManager = new ListenerManager();
		notificationQ = new LinkedBlockingDeque<>();

		//  read accounts -- TODO: this information should all be in a database not a file

		Path accountsDir = Paths.get(OS.getSettingsDir(Babbage.class).toString());
		Files.createDirectories(accountsDir);

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, Files.newInputStream(Paths.get(OS.getSettingsDir(Babbage.class).toString(), ACCOUNTS_FILE)), Lang.TURTLE);

		// For each adapter pass them the model and then let them sort it out
		Model accountsModel = createAccountsModel(model);

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
			Resource name = soln.getResource("id");
			Resource type = soln.getResource("type");
			Class<IAdapter> cls = (Class<IAdapter>) m_Class.get(type.toString());
			IAdapter adapter = cls.getConstructor(String.class, Dataset.class, ListenerManager.class, BlockingDeque.class, Model.class)
					.newInstance(name.toString(), messageDatabase, listenerManager, notificationQ, accountsModel);
			adapter.initialize();
			m_adapter.put(adapter.getId(), adapter);
		}

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
						}
						catch (RuntimeException ex)
						{
							logger.warning("Notifier thread failed :: " + ex.getLocalizedMessage());
						}
					}
				}
				catch (InterruptedException e)
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
	public Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds)
	{
		return getAccount(accountId).loadAhead(folderId, messageIds);
	}

	// Will call an addListener method in each adapter
	@Override
	public Map<String, FutureTask<?>> addListener(IStorageListener listener)
	{
		listenerManager.addListener(listener);

		for (IAdapter adapter : m_adapter.values())
		{
			// Add relevant triples to the model
			adapter.addListener();
		}

		// Start the synching of the IMAP org.knowtiphy.pinkpigmail.server, adds its work to the front of the Queue
		// but need to put these account of the Queue
		Map<String, FutureTask<?>> accountToFuture = new ConcurrentHashMap<>(100);
		for (IAdapter adapter : m_adapter.values())
		{
			FutureTask<?> futureTask = adapter.getSynchTask();
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

	@Override
	public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds)
	{
		return getAccount(accountId).deleteMessages(folderId, messageIds);
	}

	@Override
	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
										Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return getAccount(accountId).moveMessagesToJunk(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
								  String targetFolderId, boolean delete)
	{
		return getAccount(accountId).copyMessages(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
											boolean flag)
	{
		return getAccount(accountId).markMessagesAsAnswered(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds,
										boolean flag)
	{
		return getAccount(accountId).markMessagesAsRead(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
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

	@Override
	public void close()
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

	@Override
	public Future<?> send(Model model) throws StorageException
	{
		try
		{
			return m_adapter.get(getAccountId(model)).send(model);
		}
		catch (Exception ex)
		{
			throw new StorageException(ex);
		}
	}

	@Override
	public void send(MessageModel model) throws StorageException
	{
		try
		{
			Message message = m_adapter.get(model.getAccountId()).createMessage(model);
			Transport.send(message);
			if (model.getCopyToId() != null)
			{
				m_adapter.get(model.getAccountId()).appendMessages(model.getCopyToId(), new Message[]{message});
			}
		}
		catch (MessagingException | IOException ex)
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

	private Model createAccountsModel(Model model)
	{
		Model dModel = ModelFactory.createDefaultModel();
		// Theoretically I want add all various subclass of accounts here
		JenaUtils.addSubClasses(dModel, Vocabulary.IMAP_ACCOUNT, Vocabulary.ACCOUNT);
		JenaUtils.addSubClasses(dModel, Vocabulary.CALDAV_ACCOUNT, Vocabulary.ACCOUNT);
		JenaUtils.addSubClasses(dModel, Vocabulary.CARDDAV_ACCOUNT, Vocabulary.ACCOUNT);
		return ModelFactory.createRDFSModel(model);
	}

	//	extract the account id from a model
	private Resource getAccountId(Model model)
	{
		Model dModel = createAccountsModel(model);
		String getAccountType = "SELECT ?id ?type "
				+ " WHERE {" + "      ?id <" + Vocabulary.RDF_TYPE + "> ?type \n."
				+ "      ?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <" + Vocabulary.ACCOUNT + ">\n."
				+ "       filter(?type != <" + Vocabulary.ACCOUNT + ">)" + "      }";

		ResultSet result = QueryExecutionFactory.create(getAccountType, dModel).execSelect();

		if (!result.hasNext())
		{
			// throw some kind of error because we couldn't find the account
		}

		return result.next().getResource("id");
	}
}