package cn.northpark.javaSpark.TrafficProject.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.northpark.javaSpark.TrafficProject.dao.IAreaDao;
import cn.northpark.javaSpark.TrafficProject.domain.Area;
import cn.northpark.javaSpark.TrafficProject.domain.Task;
import cn.northpark.javaSpark.TrafficProject.jdbc.JDBCHelper;
import cn.northpark.javaSpark.TrafficProject.jdbc.JDBCHelper.QueryCallback;

public class AreaDaoImpl implements IAreaDao {

	public List<Area> findAreaInfo() {
		final List<Area> areas = new ArrayList<Area>();
		
		String sql = "SELECT * FROM area_info";
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, null, new QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					String areaId = rs.getString(1);
					String areaName = rs.getString(2);
					areas.add(new Area(areaId, areaName));
				}
			}
		});
		return areas;
	}

}
