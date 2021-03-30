package org.knowtiphy.babbage.storage;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseDavAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseDavAdapter.class.getName());

	private static final Runnable POISON_PILL = () -> {
	};

	private static final long FREQUENCY = 30L;

	protected final ScheduledExecutorService pingService = Executors.newSingleThreadScheduledExecutor();
	protected final BlockingQueue<Runnable> workQ = new LinkedBlockingQueue<>();

	protected final Sardine sardine;

	protected final String emailAddress;
	protected final String password;
	protected final String serverName;
	protected final String serverHeader;
	protected String nickName;
	private final Thread doWork;

	public BaseDavAdapter(String name, String type, Dataset messageDatabase, ListenerManager listenerManager,
						  BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(type, messageDatabase, listenerManager, notificationQ);

		// query for serverName, emailAdress, and password in the configuration model passed in

		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_SERVER_NAME);
		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_EMAIL_ADDRESS);
		assert JenaUtils.hasUnique(model, name, Vocabulary.HAS_PASSWORD);

		serverName = JenaUtils.getS(model, name, Vocabulary.HAS_SERVER_NAME);
		emailAddress = JenaUtils.getS(model, name, Vocabulary.HAS_EMAIL_ADDRESS);
		password = JenaUtils.getS(model, name, Vocabulary.HAS_PASSWORD);
		serverHeader = JenaUtils.getS(model, name, Vocabulary.HAS_SERVER_HEADER);
		try
		{
			this.nickName = JenaUtils.getS(model, name, Vocabulary.HAS_NICK_NAME);
		}
		catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

		sardine = SardineFactory.begin(emailAddress, password);
		doWork = new Thread(new Worker());
		doWork.start();

		//	set the id for the calendar adapter
		id = Vocabulary.E(type, emailAddress);
	}

	protected abstract void addInitialTriples(Delta delta);

	protected abstract Runnable getSyncTask();

	@Override
	public void initialize(Delta delta) throws MessagingException, IOException
	{
		addInitialTriples(delta);
		startPinger();
	}

	@Override
	public void close(Model model)
	{
		try
		{
			workQ.add(POISON_PILL);
			doWork.join();
		}
		catch (InterruptedException ex)
		{
			//  ignore
		}

		LOGGER.log(Level.INFO, "Shutting down ping service");
		IProcedure.doAndIgnore(pingService::shutdown);
		//noinspection ResultOfMethodCallIgnored
		IProcedure.doAndIgnore(() -> pingService.awaitTermination(10, TimeUnit.SECONDS));
	}

	@Override
	public void sync()
	{
		workQ.add(getSyncTask());
	}

	private void startPinger()
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "::startPinger");
		pingService.scheduleAtFixedRate(() -> workQ.add(getSyncTask()), 2 * FREQUENCY, FREQUENCY, TimeUnit.SECONDS);
		LOGGER.exiting(this.getClass().getCanonicalName(), "startPinger");
	}

	//	TODO this needs to close the result set -- and they need to be in DFetch at a guess
	protected String getStoredTag(String query, String resType)
	{
		return query(() -> {
			ResultSet resultSet = QueryExecutionFactory.create(query, cache.getDefaultModel()).execSelect();
			return JenaUtils.single(resultSet, soln -> soln.get(resType).toString());
		});
	}

	//	TODO this needs to close the result set -- and they need to be in DFetch at a guess
	protected Set<String> getStored(String query, String type)
	{
		Set<String> stored = new HashSet<>(1000);
		query(() -> {
			ResultSet resultSet = QueryExecutionFactory.create(query, cache.getDefaultModel()).execSelect();
			resultSet.forEachRemaining(soln -> stored.add(soln.get(type).asResource().toString()));
		});

		return stored;
	}

	public void updateTriple(Model messageDB, Object delta, String resURI, String hasProp, Object updated)
	{
//		delta.delete(messageDB.listStatements(R(messageDB, resURI), P(messageDB, hasProp), (RDFNode) null));
//		delta.addDP(resURI, hasProp, updated);
	}

	private class Worker implements Runnable
	{
		@Override
		public void run()
		{
			while (true)
			{
				try
				{
					Runnable task = workQ.take();

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
						catch (Exception ex)
						{
							LOGGER.warning(ex.getLocalizedMessage());
						}
					}
				}
				catch (InterruptedException e)
				{
					return;
				}
			}
		}
	}
}
