/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eva.graber;

import java.awt.AWTException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Natali
 */
public class Graber {
    
    static final int UPDATE_LAST_CANDLES = 30; // last N day
    
    static GraberFunctions gf;
    static SQLQuery sql;
    
    public static void main (String [] args) throws AWTException, SQLException, InterruptedException {
        
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        sql = new SQLQuery("bittrex");
        
        Map <String, Status> markets = sql.readMarkets();
        
        for (Map.Entry<String, Status> entry: markets.entrySet()) {
            if(entry.getValue().equals(Status.WORKING)) {
                System.out.println("\u001B[35m" + entry.getKey() + "\u001B[0m");
                List <Candle> mc = null;
                
                for(int i = 0; i < 5; i++) {
                    mc = sql.readCandles(entry.getKey(), Period.DAY);
                    if (mc.isEmpty()) {
                        Thread.sleep(60 * 1000);
                    }
                }
                
                if (mc != null) {
                    List <Long> lostCandles = new ArrayList<>();
                    // если данных по маркету нет, то собираем все данные
                    if (mc.isEmpty()) {
                        grabMarketInfo(entry.getKey(), Period.DAY);
                        grabMarketInfo(entry.getKey(), Period.HOUR);
                    }
                    /*
                    else {
                        long last = 0;
                        // Ищем пропущенные свечи если есть
                        for(Candle c: mc) {
                            if (last == 0) {
                                last = c.getTime();
                            }
                            else {
                                while (last < c.getTime()) {
                                    last = last + (24 * 60 * 60 * 1000);
                                    if (last != c.getTime()) {
                                        lostCandles.add(last);
                                    }
                                }
                            }
                        }
                    }
                    if (!lostCandles.isEmpty()) {
                        findLostMarketInfo(lostCandles, entry.getKey(), Period.DAY);
                    }
                    */
                    
                    updateMarketInfo(entry.getKey(), Period.DAY);
                    updateMarketInfo(entry.getKey(), Period.HOUR);

                }
            }
        }
    }
    
    static void grabMarketInfo (String name, Period p) throws SQLException {
        
        gf = new GraberFunctions(name);
        gf.grabInfo(0, p);
        
        Set <Candle> candles = gf.getCandles(p);
        
        for (int i = 0; i < 5; i++) {
            if (sql.writeJapaneseCandles(candles, name, p)) {
                break;
            }
            else {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Graber.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
    }
    
    // Обновление последних свечей
    static void updateMarketInfo (String name, Period p) throws SQLException {
        gf = new GraberFunctions(name);
        long minus;
        
        minus = sql.getTimeOfLastJapaneseCandle(name, p);
        minus = minus - (UPDATE_LAST_CANDLES * 24 * 60 * 60 * 1000); // 
        gf.grabInfo(minus, p);
        Set <Candle> candles = gf.getCandles(p);
        for (int i = 0; i < 5; i++) {
            if (sql.updateJapaneseCandles(candles, name, p)) {
                break;
            }
            else {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Graber.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
    }
    
    // Поиск и добавление пропущенных свечей
    static void findLostMarketInfo (List <Long> lost, String name, Period p) throws SQLException {
        gf = new GraberFunctions(name);
        gf.grabInfo(0, p);
        
        Set <Candle> candles = gf.getCandles(p);
        Set <Candle> found = new HashSet <>();
        
        // Добавляем пропущенные свечи
        for (Candle c: candles) {
            for (long time: lost) {
                if (c.getTime() == time) {
                    found.add(c);
                }
            }
        }
        
        for (int i = 0; i < 5; i++) {
            if (sql.writeJapaneseCandles(found, name, p)) {
                break;
            }
            else {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Graber.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
    }
    
}
