package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.component.VEvent;
import com.github.sardine.DavResource;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author graham
 */
public interface DStore
{
	static Resource R(Model model, String name)
	{
		return model.createResource(name);
	}

	static Property P(ModelCon model, String name)
	{
		return model.createProperty(name);
	}

	static <T> Literal L(ModelCon model, T value)
	{
		return model.createTypedLiteral(value);
	}

	static <T> Literal L(Model model, T value, RDFDatatype rdfDatatype)
	{
		return model.createTypedLiteral(value, rdfDatatype);
	}

	static <S> void attr(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	static <S> S optionalAttr(VEvent vEvent, Predicate<VEvent> predicate, Function<VEvent, S> fn)
	{
		if (predicate.test(vEvent))
		{
			return fn.apply(vEvent);
		}
		else
		{
			return null;
		}
	}

	static <S, T> void addAttribute(Delta delta, String subject, String predicate, S value, Function<S, T> fn)
	{
		if (value != null)
		{
			delta.addL(subject, predicate, fn.apply(value));
		}
	}


	static void storeCalendar(Model model, String adapterID, String encodedCalendar, DavResource calendar)
	{
		Resource calRes = model.createResource(encodedCalendar);
		model.add(calRes, model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_CALENDAR));
		model.add(model.createResource(adapterID), model.createProperty(Vocabulary.CONTAINS), calRes);
		model.add(calRes, model.createProperty(Vocabulary.HAS_NAME),
				model.createTypedLiteral(calendar.getDisplayName()));
		model.add(calRes, model.createProperty(Vocabulary.HAS_CTAG),
				model.createTypedLiteral(calendar.getCustomProps().get("getctag")));
	}

	static void storeCalendar(Delta delta, String adapterID, String encodedCalendar, DavResource calendar)
	{
		delta.addR(encodedCalendar, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR)
				.addR(adapterID, Vocabulary.CONTAINS, encodedCalendar);

		addAttribute(delta, encodedCalendar, Vocabulary.HAS_NAME, calendar.getDisplayName(), x -> x);
		addAttribute(delta, encodedCalendar, Vocabulary.HAS_CTAG, calendar.getCustomProps().get("getctag"), x -> x);
	}

	static void storeEvent(Model addModel, String calendarName, String eventName, VEvent vEvent, DavResource event)
	{
		Resource eventRes = R(addModel, eventName);
		addModel.add(eventRes, P(addModel, Vocabulary.RDF_TYPE), addModel.createResource(Vocabulary.CALDAV_EVENT));
		addModel.add(R(addModel, calendarName), P(addModel, Vocabulary.CONTAINS), eventRes);

		attr(addModel, eventRes, Vocabulary.HAS_ETAG, event.getEtag(), x -> L(addModel, x));

		attr(addModel, eventRes, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue(), x -> L(addModel, x));

		attr(addModel, eventRes, Vocabulary.HAS_DATE_START, CALDAVAdapter.fromDate(vEvent.getDateStart().getValue()),
				x -> L(addModel, new XSDDateTime(GregorianCalendar.from(x))));

		attr(addModel, eventRes, Vocabulary.HAS_DATE_END, vEvent.getDateEnd() != null ?
						CALDAVAdapter.fromDate(vEvent.getDateEnd().getValue()) :
						CALDAVAdapter.fromDate(vEvent.getDateStart().getValue())
								.plus(Duration.parse(vEvent.getDuration().getValue().toString())),
				x -> L(addModel, new XSDDateTime(GregorianCalendar.from(x))));

		attr(addModel, eventRes, Vocabulary.HAS_DESCRIPTION,
				optionalAttr(vEvent, x -> x.getDescription() != null, y -> y.getDescription().getValue()),
				x -> L(addModel, x));

		attr(addModel, eventRes, Vocabulary.HAS_PRIORITY,
				optionalAttr(vEvent, x -> x.getPriority() != null, y -> y.getPriority().getValue()),
				x -> L(addModel, x));
	}

	static void storeEvent(Delta delta, String calendarId, String eventId, VEvent vEvent, DavResource event)
	{
		delta.addR(eventId, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT)
				.addR(calendarId, Vocabulary.CONTAINS, eventId);

		addAttribute(delta, eventId, Vocabulary.HAS_ETAG, event.getEtag(), x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue(), x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_DATE_START,
				CALDAVAdapter.fromDate(vEvent.getDateStart().getValue()),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eventId, Vocabulary.HAS_DATE_END, vEvent.getDateEnd() != null ?
						CALDAVAdapter.fromDate(vEvent.getDateEnd().getValue()) :
						CALDAVAdapter.fromDate(vEvent.getDateStart().getValue())
								.plus(Duration.parse(vEvent.getDuration().getValue().toString())),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eventId, Vocabulary.HAS_DESCRIPTION,
				vEvent.getDescription() != null ? vEvent.getDescription().getValue() : null, x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_PRIORITY,
				vEvent.getPriority() != null ? vEvent.getPriority().getValue() : null, x -> x);

	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model messageDB, Model deletes, String containerName, String resName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource res = R(messageDB, resName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = messageDB.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				deletes.add(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		deletes.add(messageDB.listStatements(res, null, (RDFNode) null));
		deletes.add(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model messageDB, Delta delta, String containerName, String resName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource res = R(messageDB, resName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = messageDB.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(messageDB.listStatements(res, null, (RDFNode) null));
		delta.delete(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res));
	}

}