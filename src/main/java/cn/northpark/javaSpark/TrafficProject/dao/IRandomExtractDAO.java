package cn.northpark.javaSpark.TrafficProject.dao;

import java.util.List;

import cn.northpark.javaSpark.TrafficProject.domain.RandomExtractCar;
import cn.northpark.javaSpark.TrafficProject.domain.RandomExtractMonitorDetail;

/**
 * 随机抽取car信息管理DAO类
 * @author root
 *
 */
public interface IRandomExtractDAO {
	void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts);
	
	void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> r);
	
}
