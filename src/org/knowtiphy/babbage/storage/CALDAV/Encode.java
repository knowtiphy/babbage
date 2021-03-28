package org.knowtiphy.babbage.storage.CALDAV;

import com.github.sardine.DavResource;
import org.knowtiphy.babbage.storage.Vocabulary;

//	methods to encode javax mail objects as URIs and vica versa

public class Encode
{
	protected static String encode(DavResource calendar, String emailAddress)
	{
		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, emailAddress, calendar.getHref());
	}

	protected static String encode(DavResource calendar, DavResource event, String emailAddress)
	{
		return Vocabulary.E(Vocabulary.CALDAV_EVENT, emailAddress, calendar.getHref(), event.getHref());
	}
}
