package org.knowtiphy.babbage.storage.CALDAV;

import org.knowtiphy.babbage.storage.Vocabulary;

/**
 *
 * @author graham
 */
public interface DFetch
{
    String EVENTRES = "event";
    String CALRES = "calendar";

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

}