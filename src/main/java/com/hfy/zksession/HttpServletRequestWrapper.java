package com.hfy.zksession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * Request包装器
 */
public class HttpServletRequestWrapper extends javax.servlet.http.HttpServletRequestWrapper {

	private static final Logger logger = LoggerFactory.getLogger(HttpServletRequestWrapper.class);
	private String sessionId = "";

	public HttpServletRequestWrapper(String sessionId , HttpServletRequest request) {
		super(request);
		this.sessionId = sessionId;
	}

	@Override
	public HttpSession getSession(boolean create) {
		HttpSession session;

		if (!ZooKeeperHelper.isValid(sessionId)) {	// 在zookeeper中不存在
			session = super.getSession(create);
			if (session == null) {	// 本地不创建
				return null;
			}
			// 本地创建，存入zookeeper
			ZooKeeperHelper.createSessionNode(new SessionMetaData(this.sessionId));
		} else {	// 在zookeeper中存在，本地不存在，本地创建
			ZooKeeperHelper.updateSessionMetaData(sessionId);
			session = super.getSession(true);
		}

		return new HttpSessionSessionIdWrapper(this.sessionId, session);
	}

	@Override
	public HttpSession getSession() {
		HttpSession session = super.getSession();
		if (!ZooKeeperHelper.isValid(sessionId)) {	// 在zookeeper中不存在，创建
			ZooKeeperHelper.createSessionNode(new SessionMetaData(this.sessionId));
		} else {	// 在zookeeper中存在，更新
			ZooKeeperHelper.updateSessionMetaData(sessionId);
		}
		return new HttpSessionSessionIdWrapper(this.sessionId, session);
	}
}
