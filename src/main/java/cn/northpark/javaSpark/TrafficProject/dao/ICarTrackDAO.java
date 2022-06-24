package cn.northpark.javaSpark.TrafficProject.dao;

import java.util.List;

import cn.northpark.javaSpark.TrafficProject.domain.CarTrack;

public interface ICarTrackDAO {
	
	/**
	 * 批量插入车辆轨迹信息
	 * @param carTracks
	 */
	void insertBatchCarTrack(List<CarTrack> carTracks);
}
