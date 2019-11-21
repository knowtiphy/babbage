package org.knowtiphy.babbage.storage.CALDAV;

import org.knowtiphy.babbage.storage.Vocabulary;

/**
 *
 * @author graham
 */
public interface DFetch
{
    static String eventURIs(String calURI)
    {
        return "SELECT ?event "
                + "WHERE {"
                + "      ?event <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
                + "      <" + calURI + "> <" + Vocabulary.CONTAINS + "> ?event.\n"
                + "      }";
    }

}