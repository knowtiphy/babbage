package org.knowtiphy.babbage.storage;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.PriorityExecutor;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.utils.JenaUtils.P;
import static org.knowtiphy.utils.JenaUtils.R;

public abstract class BaseDavAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseDavAdapter.class.getName());

	private static final Runnable POISON_PILL = () -> {
	};

	private static final long FREQUENCY = 60L;

	protected final ScheduledExecutorService pingService = Executors.newSingleThreadScheduledExecutor();
	//protected final BlockingQueue<Runnable> workQ = new LinkedBlockingQueue<>();
	private final ExecutorService workService;

	protected final Sardine sardine;

	protected final String emailAddress;
	protected final String password;
	protected final String serverName;
	protected final String serverHeader;
	protected String nickName;
	//private final Thread doWork;

	public BaseDavAdapter(String name, String type, Dataset messageDatabase, ListenerManager listenerManager,
						  BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(type, messageDatabase, listenerManager, notificationQ);

		// query for serverName, emailAdress, and password in the configuration model passed in

		serverName = JenaUtils.unique(model, name, Vocabulary.HAS_SERVER_NAME, JenaUtils::getS);
		emailAddress = JenaUtils.unique(model, name, Vocabulary.HAS_EMAIL_ADDRESS, JenaUtils::getS);
		password = JenaUtils.unique(model, name, Vocabulary.HAS_PASSWORD, JenaUtils::getS);
		serverHeader = JenaUtils.unique(model, name, Vocabulary.HAS_SERVER_HEADER, JenaUtils::getS);
		nickName = JenaUtils.atMostOne(model, name, Vocabulary.HAS_NICK_NAME, JenaUtils::getS);

		sardine = SardineFactory.begin(emailAddress, password);

		workService = new PriorityExecutor();

//		doWork = new Thread(new Worker());
//		doWork.setDaemon(true);
//		doWork.start();

		//	set the id for the calendar adapter
		id = Vocabulary.E(type, emailAddress);
	}

	protected abstract void addInitialTriples(Delta delta);

	protected abstract Callable<Void> getSyncTask();

	@Override
	public void initialize(Map<String, Function<String, Future<?>>> syncs, Delta delta) throws MessagingException, IOException
	{
		addInitialTriples(delta);
		startPinger();
	}

	@Override
	public void close(Model model)
	{
		IProcedure.doAndIgnore(workService::shutdown);
		IProcedure.doAndIgnore(() -> workService.awaitTermination(10, TimeUnit.SECONDS));

//		try
//		{
//			workQ.add(POISON_PILL);
//			doWork.join();
//		}
//		catch (InterruptedException ex)
//		{
//			//  ignore
//		}

		LOGGER.log(Level.INFO, "Shutting down ping service");
		IProcedure.doAndIgnore(pingService::shutdown);
		//noinspection ResultOfMethodCallIgnored
		IProcedure.doAndIgnore(() -> pingService.awaitTermination(10, TimeUnit.SECONDS));
	}

//	@Override
//	public void sync()
//	{
//		workQ.add(getSyncTask());
//	}

	private void startPinger()
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "::startPinger");
		pingService.scheduleAtFixedRate(() -> addWork(getSyncTask()), 2 * FREQUENCY, FREQUENCY, TimeUnit.SECONDS);
		LOGGER.exiting(this.getClass().getCanonicalName(), "startPinger");
	}

	protected String getStoredTag(String query, String resType)
	{
		return query(() -> {
			try (QueryExecution qexec = QueryExecutionFactory.create(query, cache.getDefaultModel()))
			{
				ResultSet resultSet = qexec.execSelect();
				return JenaUtils.unique(resultSet, soln -> soln.get(resType).toString());
			}
		});
	}

	protected Set<String> getStored(String query, String type)
	{
		Set<String> stored = new HashSet<>(1000);
		query(() -> {
			try (QueryExecution qexec = QueryExecutionFactory.create(query, cache.getDefaultModel()))
			{
				ResultSet resultSet = qexec.execSelect();
				resultSet.forEachRemaining(soln -> stored.add(soln.get(type).asResource().toString()));
			}
		});

		return stored;
	}

	public void updateTriple(Model cache, Delta delta, String resURI, String hasProp, Object updated)
	{
		delta.delete(cache.listStatements(R(cache, resURI), P(cache, hasProp), (RDFNode) null));
		delta.addDP(resURI, hasProp, updated);
	}

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

//	private class Worker implements Runnable
//	{
//		@Override
//		public void run()
//		{
//			while (true)
//			{
//				try
//				{
//					Runnable task = workQ.take();
//
//					if (task == POISON_PILL)
//					{
//						return;
//					}
//					else
//					{
//						try
//						{
//							task.run();
//						}
//						catch (Exception ex)
//						{
//							LOGGER.warning(ex.getLocalizedMessage());
//						}
//					}
//				}
//				catch (InterruptedException e)
//				{
//					return;
//				}
//			}
//		}
//	}
}
