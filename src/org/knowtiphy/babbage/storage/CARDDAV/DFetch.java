package org.knowtiphy.babbage.storage.CARDDAV;

import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;

public interface DFetch
{
	String CARDRES = "card";
	String ABOOKRES = "addressbook";
	String ETAG = "etag";
	String CTAG = "ctag";
	String FORMATTEDNAME = "formattedName";
	String PHONENUMBER = "phoneNumber";
	String PHONETYPE = "phoneType";
	String EMAIL = "email";
	String EMAILTYPE = "emailType";
	String NAME = "name";

	// @formatter:off
	static String cardURIs(String bookURI)
	{
		return "SELECT ?" + CARDRES + " "
				+ "WHERE {"
				+ "      ?" + CARDRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_CARD + ">.\n"
				+ "      <" + bookURI + "> <" + Vocabulary.CONTAINS + "> ?" + CARDRES + ".\n"
				+ "      }";
	}

	static String addressBookURIs(String adapterURI)
	{
		return "SELECT ?" + ABOOKRES + " "
				+ "WHERE {"
				+ "      ?" + ABOOKRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_ADDRESSBOOK + ">.\n"
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

	static String cardETAG(String contactURI)
	{
		return "SELECT ?" + ETAG + " "
				+ "WHERE {"
				+ "      <" + contactURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_CARD + ">.\n"
				+ "      <" + contactURI + "> <" + Vocabulary.HAS_ETAG + "> ?" + ETAG + ".\n"
				+ "      }";
	}

	static String addressBookProperties(String addressBookURI)
	{
		return "SELECT ?name"
				+ " WHERE {"
				+ "      <" + addressBookURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_ADDRESSBOOK + ">.\n"
				+ "      <" + addressBookURI + "> <" + Vocabulary.HAS_NAME + "> ?name.\n"
				+ "      }";
	}

	static String cardProperties(String cardURI)
	{
		return "SELECT ?" + FORMATTEDNAME + " ?" + PHONENUMBER + " ?" + PHONENUMBER + " ?" + EMAIL + " ?" + EMAILTYPE + " "
				+ " WHERE {"
				+ "      <" + cardURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_CARD + ">.\n"
				+ "      <" + cardURI + "> <" + Vocabulary.HAS_FORMATTED_NAME + "> ?" + FORMATTEDNAME + ".\n"
				+ " OPTIONAL { <" + cardURI + "> <" + Vocabulary.HAS_PHONE_NUMBER + "> ?" + PHONENUMBER + " }\n"
				+ " OPTIONAL { <" + cardURI + "> <" + Vocabulary.HAS_PHONE_TYPE + "> ?" + PHONETYPE + " }\n"
				+ " OPTIONAL { <" + cardURI + "> <" + Vocabulary.HAS_EMAIL + "> ?" + EMAIL + " }\n"
				+ " OPTIONAL { <" + cardURI + "> <" + Vocabulary.HAS_EMAIL_TYPE + "> ?" + EMAILTYPE + " }\n"
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
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_CARD_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.CONTAINS, Vars.VAR_CARD_ID,
						Vars.VAR_CARD_ID, Vocabulary.HAS_FORMATTED_NAME, Vars.VAR_FORMATTED_NAME,
						Vars.VAR_CARD_ID, Vocabulary.HAS_PHONE_NUMBER, Vars.VAR_PHONE_NUMBER,
						Vars.VAR_PHONE_NUMBER, Vocabulary.HAS_PHONE_TYPE, Vars.VAR_PHONE_TYPE,
						Vars.VAR_CARD_ID, Vocabulary.HAS_EMAIL_ADDRESS, Vars.VAR_EMAIL,
						Vars.VAR_EMAIL, Vocabulary.HAS_EMAIL_TYPE, Vars.VAR_EMAIL_TYPE,
						// START OF WHERE
						Vars.VAR_CARD_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.CONTAINS, Vars.VAR_CARD_ID,
						Vars.VAR_CARD_ID, Vocabulary.HAS_FORMATTED_NAME, Vars.VAR_FORMATTED_NAME,
						Vars.VAR_CARD_ID, Vocabulary.HAS_PHONE_NUMBER, Vars.VAR_PHONE_NUMBER,
						Vars.VAR_PHONE_NUMBER, Vocabulary.HAS_PHONE_TYPE, Vars.VAR_PHONE_TYPE,
						Vars.VAR_CARD_ID, Vocabulary.HAS_EMAIL_ADDRESS, Vars.VAR_EMAIL,
						Vars.VAR_EMAIL, Vocabulary.HAS_EMAIL_TYPE, Vars.VAR_EMAIL_TYPE);
	}

}
