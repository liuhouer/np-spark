package cn.northpark.javaSpark.TrafficProject.dao;


import cn.northpark.javaSpark.TrafficProject.domain.Task;

/**
 * 任务管理DAO接口
 * @author root
 *
 */
public interface ITaskDAO {

	/**
	 * 根据task的主键查询指定的任务
	 * @param taskId
	 * @return
	 */
	Task findTaskById(long taskId);
}
