package org.knowtiphy.babbage.storage.CALDAV;

import org.knowtiphy.babbage.storage.Vocabulary;

/**
 * @author graham
 */
public interface DFetch
{
	String EVENTRES = "event";
	String CALRES = "calendar";
	String ETAG = "etag";

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
        return "SELECT ?summary ?dateStart ?dateEnd ?description ?priority"
                + " WHERE {"
                + "      <" + evenURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_SUMMARY + "> ?summary.\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_DATE_START + "> ?dateStart.\n"
                + "      <" + evenURI + "> <" + Vocabulary.HAS_DATE_END + "> ?dateEnd.\n"
                + " OPTIONAL { <" + evenURI + "> <" + Vocabulary.HAS_DESCRIPTION + "> ?description }\n"
                + " OPTIONAL { <" + evenURI + "> <" + Vocabulary.HAS_PRIORITY + "> ?priority }\n"
                + "      }";


    }
}