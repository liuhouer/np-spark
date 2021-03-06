package cn.northpark.javaSpark.TrafficProject.dao;

import java.util.List;

import cn.northpark.javaSpark.TrafficProject.domain.MonitorState;
import cn.northpark.javaSpark.TrafficProject.domain.TopNMonitor2CarCount;
import cn.northpark.javaSpark.TrafficProject.domain.TopNMonitorDetailInfo;

/**
 * 卡口流量监控管理DAO接口
 * @author root
 *
 */
public interface IMonitorDAO {
	/**
	 * 卡口流量topN批量插入到数据库
	 * @param topNMonitor2CarCounts
	 */
	void insertBatchTopN(List<TopNMonitor2CarCount> topNMonitor2CarCounts);
	
	/**
	 * 卡口下车辆具体信息插入到数据库
	 * @param monitorDetailInfos
	 */
	void insertBatchMonitorDetails(List<TopNMonitorDetailInfo> monitorDetailInfos);

	
	/**
	 * 卡口状态信息插入到数据库
	 * @param monitorState
	 */
	void insertMonitorState(MonitorState monitorState);

	void insertBatchTop10Details(List<TopNMonitorDetailInfo> topNMonitorDetailInfos);
}
