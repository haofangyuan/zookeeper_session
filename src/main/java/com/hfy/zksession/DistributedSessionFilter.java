package com.hfy.zksession;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hfy.ZooKeeperHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式session过滤器
 */
@WebFilter(filterName = "testFilter1", urlPatterns = "/*")
public class DistributedSessionFilter implements Filter {

	private final static Logger logger = LoggerFactory.getLogger(DistributedSessionFilter.class);


	public void doFilter(ServletRequest servletRequest,
			ServletResponse servletResponse, FilterChain filterChain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		HttpServletResponse response = (HttpServletResponse) servletResponse;
//		String uri = ((HttpServletRequest) request).getRequestURI();

		// 实例化了一个包装器（装饰者模式）类, 包装Request对象，用于重写Session的相关操作
		String sessionId = CookieHelper.getSessionId(request, response);
		HttpServletRequestWrapper httpServletRequestWrapper = new HttpServletRequestWrapper(sessionId, request);
		logger.debug("sessionId:"+sessionId);
        
        
		filterChain.doFilter(httpServletRequestWrapper, response);
	}


	public void init(FilterConfig filterConfig) throws ServletException {

		//初始化ZooKeeper配置参数
		ZooKeeperHelper.initialize("localhost:2181");

		//创建组节点
		ZooKeeperHelper.createSessionGroupNode();

		logger.debug("DistributedSessionFilter.init completed.");

	}

	public void destroy() {
		//销毁ZooKeeper
		ZooKeeperHelper.destroy();
		logger.debug("DistributedSessionFilter.destroy completed.");
	}

}
