/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eva.graber;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author username
 */
public class SQLQuery {
    // JDBC URL, username and password of MySQL server
    private static String url;
    
    private static String user;
    private static String password;

    // JDBC variables for opening and managing connection
    private static ResultSet rs;
    
    public SQLQuery(String db) {
        
        url = "jdbc:mariadb://79.120.44.138:33306/" + db;
        //url = "jdbc:mariadb://192.168.0.9:3306/" + db;
        user = "root";
        password = "[htydfv1303";
        
        try {
            // MariaDB
            Class.forName("org.mariadb.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
        }
    }
    
    public synchronized boolean writeJapaneseCandles(Set <Candle> jc, String tbl, Period p) {
        try (Statement stmt = DriverManager.getConnection(url + "?useSSL=false", user, password).createStatement();){
            if (jc.size() < 2) return true;
            
            String period = null;
            
            switch (p) {
                case DAY: period = "1_day"; break;
                case HOUR: period = "1_hour"; break;
                case MINUTE: period = "1_min"; break;
                default: new RuntimeException (p + " doesn't support yet.");
            }
            
            createTable(stmt, "`" + tbl + "_candles_" + period + "`", "`time` BIGINT NOT NULL UNIQUE, `in` DOUBLE, `out` DOUBLE, `low` DOUBLE,`hight` DOUBLE, `volume` DOUBLE, `id_key` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id_key`)");
            
            for (Candle c: jc) { 
                try {
                    // time, in, out, hight, low, vask, vbid
                    stmt.execute("INSERT INTO `" + tbl + "_candles_" + period + "` (`time`, `in`, `out`, `low`, `hight`, `volume`) VALUES (" +
                            c.getTime() + ", " +
                            c.getIn()+ ", " +
                            c.getOut()+ ", " +
                            c.getLow()+ ", " +
                            c.getHight()+ ", " +
                            c.getVolume()+ ");"
                    );
                } catch (SQLIntegrityConstraintViolationException ex) {
                    continue;
                } catch (SQLException ex) {
                    System.out.println(SQLQuery.class.getName() + " -> " + ex);
                    return false;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(SQLQuery.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }
    
    public synchronized boolean updateJapaneseCandles(Set <Candle> jc, String tbl, Period p) {
        try (Statement stmt = DriverManager.getConnection(url + "?useSSL=false", user, password).createStatement();) {
            if (jc.size() < 2) return true;
            
            String period = null;
            
            switch (p) {
                case DAY: period = "1_day"; break;
                case HOUR: period = "1_hour"; break;
                case MINUTE: period = "1_min"; break;
                default: new RuntimeException (p + " doesn't support yet.");
            }
            
            createTable(stmt, "`" + tbl + "_candles_" + period + "`", "`time` BIGINT NOT NULL UNIQUE, `in` DOUBLE, `out` DOUBLE, `low` DOUBLE,`hight` DOUBLE, `volume` DOUBLE, `id_key` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id_key`)");
            
            for (Candle c: jc) { 
                try {
                    // time, in, out, hight, low, vask, vbid
                    stmt.execute("UPDATE `" + tbl + "_candles_" + period + "` SET "
                            + "`in`=" + c.getIn()+ ", "
                            + "`out`=" + c.getOut()+ ", "
                            + "`low`=" + c.getLow()+ ", "
                            + "`hight`=" + c.getHight()+ ", "
                            + "`volume`=" + c.getVolume()
                            + " WHERE `time`=" + c.getTime() + ";"
                    );
                } catch (SQLIntegrityConstraintViolationException ex) {
                    continue;
                } catch (SQLException ex) {
                    System.out.println(SQLQuery.class.getName() + " -> " + ex);
                    return false;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(SQLQuery.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }
    
    public synchronized Map <String, Status> readMarkets(){
        
        Map <String, Status> markets = new HashMap <>();
        
        try (Statement stmt = DriverManager.getConnection(url + "?useSSL=false", user, password).createStatement()) {
            
            createTable(stmt, "`markets`", "`name` VARCHAR(11) NOT NULL UNIQUE, `status` TEXT, `id_key` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id_key`)");
            
            ResultSet rs = stmt.executeQuery("SELECT `name`, `status` FROM `markets` WHERE `status` != \"" + Status.DELETED + "\";");
            
            while (rs.next()) {
                markets.put(rs.getString("name"), Status.valueOf(rs.getString("status")));
            }
            
        } catch (SQLException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
            return markets;
        }
        
        return markets;
    }
    
    public synchronized List <Candle> readCandles(String tbl, Period p) {
        
        List <Candle> candles = new ArrayList <>();
        
        String period = null;
            
        switch (p) {
            case DAY: period = "1_day"; break;
            case HOUR: period = "1_hour"; break;
            case MINUTE: period = "1_min"; break;
            default: new RuntimeException (p + " doesn't support yet.");
        }
        
        try (Statement stmt = DriverManager.getConnection(url + "?useSSL=false", user, password).createStatement()) {
            
            createTable(stmt, "`" + tbl + "_candles_" + period + "`", "`time` BIGINT NOT NULL UNIQUE, `in` DOUBLE, `out` DOUBLE, `low` DOUBLE,`hight` DOUBLE, `volume` DOUBLE, `id_key` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id_key`)");
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tbl + "_candles_" + period +  "` ORDER BY `time` ASC;");
            
            while (rs.next()) {
                Candle c = new Candle(
                    rs.getLong("time"),
                    rs.getDouble("hight"),
                    rs.getDouble("low"),
                    rs.getDouble("in"),
                    rs.getDouble("out"),
                    rs.getDouble("volume")
                );
                candles.add(c);
            }
            
        } catch (SQLException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
            return candles;
        }
        
        return candles;
    }
    
    // Создаем таблицу если она не существует
    public void createTable (Statement stmt, String data_table, String param){
        try {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + data_table + " (" + param +  ");");
        } catch (SQLException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
            return;
        }
    }
    
    
    // Взять время последней свечи в БД
    public synchronized long getTimeOfLastJapaneseCandle(String tbl, Period p) {
        
        String period = null;
            
        switch (p) {
            case DAY: period = "1_day"; break;
            case HOUR: period = "1_hour"; break;
            case MINUTE: period = "1_min"; break;
            default: new RuntimeException (p + " doesn't support yet.");
        }
        
        try {
            Statement stmt = DriverManager.getConnection(url + "?useSSL=false", user, password).createStatement();
            createTable(stmt, "`" + tbl + "_candles_" + period + "`", "`time` BIGINT NOT NULL UNIQUE, `in` DOUBLE, `out` DOUBLE, `low` DOUBLE,`hight` DOUBLE, `volume` DOUBLE, `id_key` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id_key`)");
            ResultSet rs = stmt.executeQuery("SELECT `time` FROM `" + tbl + "_candles_" + period + "` ORDER BY `time` DESC LIMIT 1;");
            
            if(rs.next()) {
                return rs.getLong("time");
            }
            
        } catch (SQLSyntaxErrorException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
        } catch (SQLException ex) {
            System.out.println(SQLQuery.class.getName() + " -> " + ex);
        } 
        return 0;
    }
    
    
}
