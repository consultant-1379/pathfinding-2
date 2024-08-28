package com.ericsson.eniq.parser;

import java.io.StringReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class EntityResolver extends DefaultHandler {

	private static final Logger logger = LogManager.getLogger(EntityResolver.class);

	public InputSource resolveEntity(final String publicId, String systemId) throws SAXException {
		logger.debug("Resolve entity \"" + systemId + "\"");

		try {

			if (systemId.lastIndexOf("/") >= 0) {
				systemId = systemId.substring(systemId.lastIndexOf("/") + 1);
			}

			logger.debug("DTD filename \"" + systemId + "\"");

			InputSource source = new InputSource(getClass().getClassLoader().getResourceAsStream(systemId));
			source.setPublicId(publicId);
			source.setSystemId(systemId);

			return source;

		} catch (Exception e) {
			logger.error("ResolveEntity failed", e);
			return returnBogus(publicId, systemId);
		}

	}

	private InputSource returnBogus(final String publicId, final String systemId) {
		final StringReader reader = new StringReader("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

		final InputSource source = new InputSource(reader);
		source.setPublicId(publicId);
		source.setSystemId(systemId);

		return source;
	}

}
