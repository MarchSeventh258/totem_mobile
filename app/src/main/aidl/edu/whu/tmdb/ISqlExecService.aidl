package edu.whu.tmdb;

interface ISqlExecService {
    String executeSql(String sql);
    String[] executeBatch(in String[] sqlList);
}
