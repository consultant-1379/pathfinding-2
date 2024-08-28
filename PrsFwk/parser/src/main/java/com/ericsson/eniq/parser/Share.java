package com.ericsson.eniq.parser;


import java.util.HashMap;
import java.util.Map;

/**
 * @author savinen
 */
public class Share {

	private final Map<String,Object> map;
	private static Share singletonPluginShare = null;

	/**
	 * In the first call of this method new share is created and returned. After
	 * the first call the same share is returned.
	 * 
	 * @return share
	 */
	public synchronized static Share instance() {

		if (singletonPluginShare == null) {
			singletonPluginShare = new Share();
		}

		return singletonPluginShare;
	}

	/**
	 * constructor
	 * 
	 */
	private Share() {
		map = new HashMap<String,Object>();
	}

	/**
	 * 
	 * Add a object to the share.
	 * 
	 * @param key
	 * @param obj
	 */
	public synchronized void add(final String key, final Object obj) {
		if (map != null) {
			map.put(key, obj);
		}
	}

	/**
	 * Remove object from the share.
	 * 
	 * @param key
	 * @return
	 */
	public synchronized Object remove(final String key) {

		if (map != null) {
			if (map.containsKey(key)) {
				return map.remove(key);
			}
		}

		return null;
	}

	/**
	 * 
	 * Fetchs a keys object from the share. This does not remove the object.
	 * 
	 * @param key
	 * @return Object
	 */
	public synchronized Object get(final String key) {

		if (map != null) {
			if (map.containsKey(key)) {
				return map.get(key);
			}
		}

		return null;
	}

	/**
	 * 
	 * Returns the size of the map.
	 * 
	 * @param key
	 * @return
	 */
	public synchronized int size() {
		
		if (map != null) {
			return map.size();
		}

		return -1;

	}

	/**
	 * 
	 * Return true if key is in the the share.
	 * 
	 * @param key
	 * @return
	 */
	public synchronized boolean contains(final String key) {

		if (map != null) {
			if (map.containsKey(key)) {
				return true;
			}
		}

		return false;

	}

}
