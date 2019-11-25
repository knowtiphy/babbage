package org.knowtiphy.babbage.storage.CALDAV;

import org.knowtiphy.babbage.storage.Vocabulary;

import java.util.HashSet;
import java.util.Set;

/**
 * @author graham
 */
public interface DFetch
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

}