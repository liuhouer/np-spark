package cn.northpark.javaSpark.TrafficProject.dao.factory;

import cn.northpark.javaSpark.TrafficProject.dao.IAreaDao;
import cn.northpark.javaSpark.TrafficProject.dao.ICarTrackDAO;
import cn.northpark.javaSpark.TrafficProject.dao.IMonitorDAO;
import cn.northpark.javaSpark.TrafficProject.dao.IRandomExtractDAO;
import cn.northpark.javaSpark.TrafficProject.dao.ITaskDAO;
import cn.northpark.javaSpark.TrafficProject.dao.IWithTheCarDAO;
import cn.northpark.javaSpark.TrafficProject.dao.impl.AreaDaoImpl;
import cn.northpark.javaSpark.TrafficProject.dao.impl.CarTrackDAOImpl;
import cn.northpark.javaSpark.TrafficProject.dao.impl.MonitorDAOImpl;
import cn.northpark.javaSpark.TrafficProject.dao.impl.RandomExtractDAOImpl;
import cn.northpark.javaSpark.TrafficProject.dao.impl.TaskDAOImpl;
import cn.northpark.javaSpark.TrafficProject.dao.impl.WithTheCarDAOImpl;

/**
 * DAO工厂类
 * @author root
 *
 */
public class DAOFactory {
	
	
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static IMonitorDAO getMonitorDAO(){
		return new MonitorDAOImpl();
	}
	
	public static IRandomExtractDAO getRandomExtractDAO(){
		return new RandomExtractDAOImpl();
	}
	
	public static ICarTrackDAO getCarTrackDAO(){
		return new CarTrackDAOImpl();
	}
	
	public static IWithTheCarDAO getWithTheCarDAO(){
		return new WithTheCarDAOImpl();
	}

	public static IAreaDao getAreaDao() {
		return  new AreaDaoImpl();
		
	}
}
