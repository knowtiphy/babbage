package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.component.VEvent;
import com.github.sardine.DavResource;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.function.Function;

import static org.knowtiphy.utils.JenaUtils.P;
import static org.knowtiphy.utils.JenaUtils.R;

/**
 * @author frank ;D
 */
public interface DStore
{
	static <S, T> void addAttribute(Delta delta, String s, String p, S value, Function<S, T> f)
	{
		if (value != null)
		{
			delta.addDP(s, p, f.apply(value));
		}
	}

	static void addCalendar(Delta delta, String aid, String cid, DavResource calendar)
	{
		delta.addType(cid, Vocabulary.CALDAV_CALENDAR)
				.addOP(aid, Vocabulary.CONTAINS, cid);

		addAttribute(delta, cid, Vocabulary.HAS_NAME, calendar.getDisplayName(), x -> x);
		addAttribute(delta, cid, Vocabulary.HAS_CTAG, calendar.getCustomProps().get("getctag"), x -> x);
	}

	static void addEvent(Delta delta, String cid, String eid, VEvent vEvent, DavResource event)
	{
		delta.addOP(eid, RDF.type.toString(), Vocabulary.CALDAV_EVENT)
				.addOP(cid, Vocabulary.CONTAINS, eid);

		addAttribute(delta, eid, Vocabulary.HAS_ETAG, event.getEtag(), x -> x);

		addAttribute(delta, eid, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue(), x -> x);

		addAttribute(delta, eid, Vocabulary.HAS_DATE_START,
				CALDAVAdapter.fromDate(vEvent.getDateStart().getValue()),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eid, Vocabulary.HAS_DATE_END, vEvent.getDateEnd() != null ?
						CALDAVAdapter.fromDate(vEvent.getDateEnd().getValue()) :
						CALDAVAdapter.fromDate(vEvent.getDateStart().getValue())
								.plus(Duration.parse(vEvent.getDuration().getValue().toString())),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eid, Vocabulary.HAS_DESCRIPTION,
				vEvent.getDescription() != null ? vEvent.getDescription().getValue() : null, x -> x);

		addAttribute(delta, eid, Vocabulary.HAS_PRIORITY,
				vEvent.getPriority() != null ? vEvent.getPriority().getValue() : null, x -> x);

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