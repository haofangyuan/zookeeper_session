package com.hfy.zksession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpSession;


public class HttpSessionSessionIdWrapper extends HttpSessionWrapper {
	
	
	private final static Logger logger = LoggerFactory
			.getLogger(HttpSessionSessionIdWrapper.class);

	private String sessionId;

	public HttpSessionSessionIdWrapper(String sessionId, HttpSession session) {
		super(session);
		this.sessionId = sessionId;
	}

	@Override
	public String getId() {
		return sessionId;
	}

	@Override
	public void setMaxInactiveInterval(int maxIdle) {
		super.setMaxInactiveInterval(maxIdle);
		if (!StringUtils.isEmpty(sessionId)) {
			ZooKeeperHelper.setMaxInactiveInterval(sessionId, maxIdle * 1000l);
		}
	}

	@Override
	public Object getAttribute(String key) {
		if (!StringUtils.isEmpty(sessionId)) {
			//返回Session节点下的数据
			return ZooKeeperHelper.getSessionData(sessionId, key);
		}
		return null;
	}

	/*public Enumeration getAttributeNames() {
		Map<String, String> session = dSessionService
				.getSession(sessionId);
		return (new Enumerator(session.keySet(), true));
	}*/

	@Override
	public void invalidate() {
		if (!StringUtils.isEmpty(sessionId)) {
			//删除Session节点
			ZooKeeperHelper.deleteSessionNode(sessionId);
		}
	}

	public void removeAttribute(String key) {
		if (!StringUtils.isEmpty(sessionId)) {
			//删除Session节点下的数据
			ZooKeeperHelper.removeSessionData(sessionId, key);
		}
	}

	@Override
	public void setAttribute(String key, Object value) {
		if (!StringUtils.isEmpty(sessionId)) {
			//将数据添加到ZooKeeper服务器上
			ZooKeeperHelper.setSessionData(sessionId, key, value);
		}
	}


}
