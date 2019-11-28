package org.knowtiphy.babbage.storage.CALDAV;

import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.util.HashSet;
import java.util.Set;

/**
 * @author frank >=)
 */
public interface Fetch
{
	String EVENTRES = "event";
	String CALRES = "calendar";
	String ETAG = "etag";
	String CTAG = "ctag";
	String SUMMARY = "summary";
	String DATESTART = "dateStart";
	String DATEEND = "dateEnd";
	String DESCRIPTION = "description";
	String PRIORITY = "priority";
	String NAME = "name";

	Set<String> eventProps = new HashSet<>();

	// @formatter:off
    static String eventURIs(String calURI)
    {
        return "SELECT ?" + EVENTRES + " "
                + "WHERE {"
                + "      ?" + EVENTRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
                + "      <" + calURI + "> <" + Vocabulary.CONTAINS + "> ?event.\n"
                + "      }";
    }

    static String calendarURIs(String adapterURI)
    {
        return "SELECT ?" + CALRES + " "
                + "WHERE {"
                + "      ?" + CALRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_CALENDAR + ">.\n"
                + "          <" + adapterURI + "> <" + Vocabulary.CONTAINS + "> ?calendar.\n"
                + "          }";
    }

    static String calendarCTag(String calendarURI)
    {
        return "SELECT ?" + CTAG + " "
                + "WHERE {"
                + "      <" + calendarURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_CALENDAR + ">.\n"
                + "      <" + calendarURI + "> <" + Vocabulary.HAS_CTAG + "> ?" + CTAG + ".\n"
                + "      }";
    }

    static String calendarProperties(String calendarURI)
    {
        return "SELECT ?name"
                + " WHERE {"
                + "      <" + calendarURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_CALENDAR + ">.\n"
                + "      <" + calendarURI + "> <" + Vocabulary.HAS_NAME + "> ?name.\n"
                + "      }";
    }

    static String eventETag(String eventURI)
    {
        return "SELECT ?" + ETAG + " "
                + "WHERE {"
                + "      <" + eventURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
                + "      <" + eventURI + "> <" + Vocabulary.HAS_ETAG + "> ?" + ETAG + ".\n"
                + "      }";
    }


    static String eventProperties(String evenURI)
    {
        return "SELECT ?" + SUMMARY + " ?" + DATESTART + " ?" + DATEEND + " ?" + DESCRIPTION + " ?" + PRIORITY + " "
                + " WHERE {"
                + "      <" + evenURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_SUMMARY + "> ?" + SUMMARY + ".\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_DATE_START + "> ?" + DATESTART + ".\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_DATE_END + "> ?" + DATEEND + ".\n"
                + " OPTIONAL { <" + evenURI + "> <" + Vocabulary.HAS_DESCRIPTION + "> ?" + DESCRIPTION + " }\n"
                + " OPTIONAL { <" + evenURI + "> <" + Vocabulary.HAS_PRIORITY + "> ?" + PRIORITY + " }\n"
                + "      }";
    }

    static String skeleton()
	{
		return String
				.format("      CONSTRUCT {   ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .  " +
								"?%s <%s> ?%s}\n " +
								"WHERE {     ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .  " +
								"?%s <%s> ?%s .  " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_CALENDAR_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_CALENDAR_ID,
						Vars.VAR_CALENDAR_ID, Vocabulary.HAS_NAME, Vars.VAR_CALENDAR_NAME,
						// START OF WHERE
						Vars.VAR_CALENDAR_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_CALENDAR_ID,
						Vars.VAR_CALENDAR_ID, Vocabulary.HAS_NAME, Vars.VAR_CALENDAR_NAME);
	}

	static String initialState()
	{
		return String
				.format("      CONSTRUCT {   ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s}\n   " +

								"WHERE {     ?%s <%s> <%s> .  " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_EVENT_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT,
						Vars.VAR_CALENDAR_ID, Vocabulary.CONTAINS, Vars.VAR_EVENT_ID,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_SUMMARY, Vars.VAR_SUMMARY,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_START, Vars.VAR_DATE_START,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_END, Vars.VAR_DATE_END,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DESCRIPTION, Vars.VAR_DESCRIPTION,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_PRIORITY, Vars.VAR_PRIORITY,
						// START OF WHERE
						Vars.VAR_EVENT_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT,
						Vars.VAR_CALENDAR_ID, Vocabulary.CONTAINS, Vars.VAR_EVENT_ID,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_SUMMARY, Vars.VAR_SUMMARY,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_START, Vars.VAR_DATE_START,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_END, Vars.VAR_DATE_END,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DESCRIPTION, Vars.VAR_DESCRIPTION,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_PRIORITY, Vars.VAR_PRIORITY);
	}
}