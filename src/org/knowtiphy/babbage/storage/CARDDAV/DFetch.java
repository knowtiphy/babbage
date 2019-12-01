package org.knowtiphy.babbage.storage.CARDDAV;

import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;

public interface DFetch
{
	String CARDRES = "card";
	String ABOOKRES = "addressbook";
	String ETAG = "etag";
	String CTAG = "ctag";

	// @formatter:off
	static String cardURIs(String bookURI)
	{
		return "SELECT ?" + CARDRES + " "
				+ "WHERE {"
				+ "      ?" + CARDRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
				+ "      <" + bookURI + "> <" + Vocabulary.CONTAINS + "> ?" + CARDRES + ".\n"
				+ "      }";
	}

	static String addressBookURIs(String adapterURI)
	{
		return "SELECT ?" + ABOOKRES + " "
				+ "WHERE {"
				+ "      ?" + ABOOKRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_CALENDAR + ">.\n"
				+ "          <" + adapterURI + "> <" + Vocabulary.CONTAINS + "> ?" + ABOOKRES + ".\n"
				+ "          }";
	}

	static String addressBookCTAG(String addressBookURI)
	{
		return "SELECT ?" + CTAG + " "
				+ "WHERE {"
				+ "      <" + addressBookURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_ADDRESSBOOK + ">.\n"
				+ "      <" + addressBookURI + "> <" + Vocabulary.HAS_CTAG + "> ?" + CTAG + ".\n"
				+ "      }";
	}

	static String contactETAG(String contactURI)
	{
		return "SELECT ?" + ETAG + " "
				+ "WHERE {"
				+ "      <" + contactURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_CARD + ">.\n"
				+ "      <" + contactURI + "> <" + Vocabulary.HAS_ETAG + "> ?" + ETAG + ".\n"
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
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_ADDRESSBOOK,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_ADDRESSBOOK_ID,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.HAS_NAME, Vars.VAR_ADDRESSBOOK_NAME,
						// START OF WHERE
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_ADDRESSBOOK,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_ADDRESSBOOK_ID,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.HAS_NAME, Vars.VAR_ADDRESSBOOK_NAME);
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
