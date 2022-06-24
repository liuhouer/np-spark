package cn.northpark.javaSpark.TrafficProject.dao.impl;

import cn.northpark.javaSpark.TrafficProject.constant.Constants;
import cn.northpark.javaSpark.TrafficProject.dao.IWithTheCarDAO;
import cn.northpark.javaSpark.TrafficProject.jdbc.JDBCHelper;
import cn.northpark.javaSpark.TrafficProject.util.DateUtils;

public class WithTheCarDAOImpl implements IWithTheCarDAO {

	@Override
	public void updateTestData(String cars) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String sql = "UPDATE task set task_param = ? WHERE task_id = 3";
		Object[] params = new Object[]{"{\"startDate\":[\""+DateUtils.getTodayDate()+"\"],\"endDate\":[\""+DateUtils.getTodayDate()+"\"],\""+Constants.FIELD_CARS+"\":[\""+cars+"\"]}"};
		jdbcHelper.executeUpdate(sql, params);
	}

}
