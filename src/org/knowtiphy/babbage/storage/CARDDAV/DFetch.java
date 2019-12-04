package org.knowtiphy.babbage.storage.CARDDAV;

import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;

public interface DFetch
{
	String CARDRES = "card";
	String GROUPRES = "group";
	String ABOOKRES = "addressbook";
	String ETAG = "etag";
	String CTAG = "ctag";
	String FORMATTEDNAME = "formattedName";
	String PHONE = "phone";
	String PHONENUMBER = "phoneNumber";
	String PHONETYPE = "phoneType";
	String EMAIL = "email";
	String EMAILADDRESS = "emailAddress";
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
				+ "      <" + contactURI + "> <" + Vocabulary.RDF_TYPE + "> ?type.\n"
				+ "      <" + contactURI + "> <" + Vocabulary.HAS_ETAG + "> ?" + ETAG + ".\n"
				+ " FILTER ( ?type = <" + Vocabulary.CARDDAV_CARD + "> || ?type = <" + Vocabulary.CARDDAV_GROUP + "> )\n"
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

	static String groupURIs(String bookURI)
	{
		return "SELECT ?" + GROUPRES + " "
				+ "WHERE {"
				+ "      ?" + GROUPRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_GROUP + ">.\n"
				+ "      <" + bookURI + "> <" + Vocabulary.HAS_GROUP + "> ?" + GROUPRES + ".\n"
				+ "      }";
	}

	static String memberCardURI(String groupURI)
	{
		return "SELECT ?" + CARDRES + " "
				+ " WHERE {"
				+ " ?" + CARDRES + " <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_CARD + ">.\n"
				+ " ?" + CARDRES + " <" + Vocabulary.HAS_UID + "> ?uid.\n"
				+ " { SELECT ?uid "
				+ "   WHERE { "
				+ "   		<" + groupURI + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CARDDAV_GROUP + ">.\n"
				+ "			<" + groupURI + "> <" + Vocabulary.HAS_MEMBER_UID + "> ?uid.\n"
				+ "			}\n"
				+ "  }\n"
				+ "	      }";
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
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_CARD_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.CONTAINS, Vars.VAR_CARD_ID,
						Vars.VAR_CARD_ID, Vocabulary.HAS_FORMATTED_NAME, Vars.VAR_FORMATTED_NAME,
						Vars.VAR_CARD_ID, Vocabulary.HAS_PHONE, Vars.VAR_PHONE_ID,
						Vars.VAR_PHONE_ID, Vocabulary.HAS_NUMBER, Vars.VAR_PHONE_NUMBER,
						Vars.VAR_PHONE_ID, Vocabulary.HAS_TYPE, Vars.VAR_PHONE_TYPE,
						Vars.VAR_CARD_ID, Vocabulary.HAS_EMAIL, Vars.VAR_EMAIL_ID,
						Vars.VAR_EMAIL_ID, Vocabulary.HAS_EMAIL_ADDRESS, Vars.VAR_EMAIL_ADDRESS,
						Vars.VAR_EMAIL_ID, Vocabulary.HAS_EMAIL_TYPE, Vars.VAR_EMAIL_TYPE,
						// START OF WHERE
						Vars.VAR_CARD_ID, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD,
						Vars.VAR_ADDRESSBOOK_ID, Vocabulary.CONTAINS, Vars.VAR_CARD_ID,
						Vars.VAR_CARD_ID, Vocabulary.HAS_FORMATTED_NAME, Vars.VAR_FORMATTED_NAME,
						Vars.VAR_CARD_ID, Vocabulary.HAS_PHONE, Vars.VAR_PHONE_ID,
						Vars.VAR_PHONE_ID, Vocabulary.HAS_NUMBER, Vars.VAR_PHONE_NUMBER,
						Vars.VAR_PHONE_ID, Vocabulary.HAS_TYPE, Vars.VAR_PHONE_TYPE,
						Vars.VAR_CARD_ID, Vocabulary.HAS_EMAIL, Vars.VAR_EMAIL_ID,
						Vars.VAR_EMAIL_ID, Vocabulary.HAS_EMAIL_ADDRESS, Vars.VAR_EMAIL_ADDRESS,
						Vars.VAR_EMAIL_ID, Vocabulary.HAS_EMAIL_TYPE, Vars.VAR_EMAIL_TYPE);
	}

}
