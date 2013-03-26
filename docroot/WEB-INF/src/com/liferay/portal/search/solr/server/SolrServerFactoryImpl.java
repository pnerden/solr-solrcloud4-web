/**
 * Copyright (c) 2000-2012 Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.portal.search.solr.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;

/**
 * @author Bruno Farache
 * @author Patrick NERDEN
 */
public class SolrServerFactoryImpl implements SolrServerFactory {

	public SolrServerFactoryImpl(Map<String, SolrServer> solrServers) {
		for (Map.Entry<String, SolrServer> entry : solrServers.entrySet()) {
			String id = entry.getKey();
			SolrServer solrServer = entry.getValue();

			SolrServerWrapper solrServerWrapper = new SolrServerWrapper(
				id, solrServer);

			solrServerWrapper.setSolrServerFactory(this);

			_liveServers.put(id, solrServerWrapper);
		}
	}

	public List<SolrServerWrapper> getDeadServers() {
		synchronized (this) {
			return new ArrayList<SolrServerWrapper>(_deadServers.values());
		}
	}

	public SolrServerWrapper getLiveServer() throws SolrServerException {
		List<SolrServerWrapper> liveServers = getLiveServers();

		SolrServerWrapper solrServerWrapper = _solrServerSelector.select(
			liveServers);

		if (solrServerWrapper != null) {
			return solrServerWrapper;
		}

		throw new SolrServerException("No server available");
	}

	public List<SolrServerWrapper> getLiveServers() {
		synchronized (this) {
			return new ArrayList<SolrServerWrapper>(_liveServers.values());
		}
	}

	public void killServer(SolrServerWrapper solrServerWrapper) {		
		synchronized (this) {
			if (_deadServers.containsKey(solrServerWrapper.getId())) {
				_log.info("SolrServer id "+solrServerWrapper.getId()+" is still down");
				return;
			}
			_log.info("Attempting to kill SolrServer id "+solrServerWrapper.getId());

			_deadServers.put(solrServerWrapper.getId(), solrServerWrapper);
			_liveServers.remove(solrServerWrapper.getId());
			
			_log.info("SolrServer id "+solrServerWrapper.getId()+" killed after "+solrServerWrapper.getInvocationCount()+" invocations");
		}
	}

	public void setSolrServerSelector(SolrServerSelector solrServerSelector) {
		_solrServerSelector = solrServerSelector;
	}

	public void startServer(SolrServerWrapper solrServerWrapper) {		
		synchronized (this) {
			if (_liveServers.containsKey(solrServerWrapper.getId())) {
				_log.debug("SolrServer id "+solrServerWrapper.getId()+" is already up. Has currently undergone "+solrServerWrapper.getInvocationCount()+" invocations");
				return;
			}
			
			_log.debug("Attempting to start SolrServer id "+solrServerWrapper.getId());

			_deadServers.remove(solrServerWrapper.getId());
			_liveServers.put(solrServerWrapper.getId(), solrServerWrapper);

			solrServerWrapper.resetInvocationCount();
			_log.debug("SolrServer id "+solrServerWrapper.getId()+" started");
		}
	}
	
	private static Log _log = LogFactoryUtil.getLog(SolrServerFactoryImpl.class);

	private Map<String, SolrServerWrapper> _deadServers =
		new HashMap<String, SolrServerWrapper>();
	private Map<String, SolrServerWrapper> _liveServers =
		new HashMap<String, SolrServerWrapper>();
	private SolrServerSelector _solrServerSelector;

}