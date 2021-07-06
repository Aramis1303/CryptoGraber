/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eva.graber;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

/**
 *
 * @author Natali
 */
public class GraberFunctions {
    
    private final int STEP = 1;     //step
    private final int DELAY = 35;   //ms
    private final int PREPARE_DELAY = 1500;   //ms
    private final int PRE_START_DELAY = 3500;   //ms
    
    private String market;
    
    private String driverPath;
    private String url;
    
    private Set <Candle> candles1H;
    private Set <Candle> candles1D;
    
    private WebDriver driver;
    
    private Robot robot;
    
    private SimpleDateFormat dateFormatHour;
    private SimpleDateFormat dateFormatDay;
    
    private boolean isFinish = false;
    private boolean isEmptyLine = false;
    
    private long lastTime;
    
    private Calendar calendar;
    
    public GraberFunctions (String market) {
        this.market = market;
        
        calendar = new GregorianCalendar();
        
        candles1H = new HashSet <>();
        candles1D = new HashSet <>();
        
        dateFormatHour = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        dateFormatDay = new SimpleDateFormat("MM-dd-yyyy");
        
        driverPath = System.getProperty("user.dir") + "/chromedriver.exe";
        
        url = "https://international.bittrex.com/Market/Index?MarketName=";
        
        System.setProperty("webdriver.chrome.driver", driverPath);
    }
    
    // Собрать инфо
    public void grabInfo(long lastSqlDate, Period period) {
        
        try {
            
            calendar.setTime(new Date());
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH);
            
            robot = new Robot();
            
            if (period.equals(Period.DAY)) {
                candles1D.clear();
            } else if (period.equals(Period.HOUR)) {
                candles1H.clear();
            }
            
            driver = new ChromeDriver();
            driver.get(url + market);
            driver.manage().window().maximize();
            Thread.sleep(PREPARE_DELAY);
            
            try{
                driver.findElement(By.xpath("//span[@class='button-group']/button[@class='cookie-banner-accept']")).click();
                Thread.sleep(1000);
            } catch (NoSuchElementException e) {}
            
            Thread.sleep(PRE_START_DELAY);
            
            driver.findElement(By.xpath("//cq-clickable[@stxbind='Layout.periodicity']")).click();
            Thread.sleep(100);
            if (period.equals(Period.DAY)) {
                driver.findElement(By.xpath("//cq-item[@stxtap=\"Layout.setPeriodicity(1, 'day')\"]")).click();
                
            } else if (period.equals(Period.HOUR)) {
                driver.findElement(By.xpath("//cq-item[@stxtap='Layout.setPeriodicity(1, 60)']")).click();
            }
            Thread.sleep(1000);
            
            int left = driver.findElement(By.xpath("//div[@class='chartContainer']")).getLocation().x;
            int top = driver.findElement(By.xpath("//div[@class='chartContainer']")).getLocation().y;
            int right = driver.findElement(By.xpath("//div[@class='chartContainer']")).getSize().width + left;
            int down = driver.findElement(By.xpath("//div[@class='chartContainer']")).getSize().height + top;
            
            int leftWork = left + (right - left)/10;
            int rightWork = right - (right - left)/10;
                    
            driver.findElement(By.xpath("//div[@class='chartContainer']"));

            int xCenter = top + ((down - top) * 2/3);
            
            isFinish = false;
            int i = right;
            long newTime = 0;
            boolean isFirst = true; // Не записываем первую свечу, т.к. она может быть ещё не сформированной
            while(!isFinish) {
                isEmptyLine = true;
                
                while (i > leftWork) {
                    robot.mouseMove(i, xCenter);
                    
                    Thread.sleep(DELAY);
                    
                    // Если поле с датой и поле с данными свечи не пустые то:
                    if (!(driver.findElement(By.tagName("cq-hu-high")).getText().isEmpty() || driver.findElement(By.xpath("//div[@class='stx-float-date floatDate']")).getText().isEmpty())) 
                    {
                        
                        if (period.equals(Period.DAY)) {
                            newTime = dateFormatDay.parse(driver.findElement(By.xpath("//div[@class='stx-float-date floatDate']")).getText()).getTime();
                        } else if (period.equals(Period.HOUR)) {
                            newTime = dateFormatHour.parse(Integer.toString(year) + "/" + driver.findElement(By.xpath("//div[@class='stx-float-date floatDate']")).getText()).getTime();
                            // Все манипуляции с календарём нужны, потому что не учитывается год, а следовательно его добавляем руками, и при переходе на прошлый год исключаем сбой через череду сравнений дат
                            calendar.setTime(new Date(newTime));
                            if (calendar.get(Calendar.MONTH) == 12 && month == 1) {
                                year --;
                            }
                            else month = calendar.get(Calendar.MONTH);
                        }
                        
                        // Отбой по указанной дате (перестаем собирать информацию)
                        if (newTime < lastSqlDate) {
                            isFinish = true;
                            break;
                        }
                        
                        // Первую свечу пропускаем, т.к. ещё не сформирована
                        if (isFirst) {
                            isFirst = false;
                        }
                        else {
                            if (lastTime != newTime) {
                                lastTime = newTime;
                                
                                double hi = Double.parseDouble(driver.findElement(By.tagName("cq-hu-high")).getText());
                                double lo = Double.parseDouble(driver.findElement(By.tagName("cq-hu-low")).getText());
                                double in = Double.parseDouble(driver.findElement(By.tagName("cq-hu-open")).getText());
                                double ou = Double.parseDouble(driver.findElement(By.tagName("cq-hu-close")).getText());
                                double vo = Double.parseDouble(driver.findElement(By.tagName("cq-hu-volume")).getText());
                                
                                if (hi != 0 && lo != 0 && in != 0 && ou != 0) { 
                                    Candle tmpc = new Candle(
                                        newTime,
                                        hi,
                                        lo,
                                        in,
                                        ou,
                                        vo 
                                    );
                                
                                    if (period.equals(Period.DAY)) {
                                        candles1D.add(tmpc);
                                    } else if (period.equals(Period.HOUR)) {
                                        candles1H.add(tmpc);
                                    }
                                }
                                isEmptyLine = false;
                            }
                        }
                    }
                    else {
                        // Если данных нет на CANVAS в том месте где они должны быть, то данные закончились
                        if ( leftWork < i && i < (rightWork - (rightWork - leftWork) /2) ) {
                            break;
                        }
                    }
                    
                    i -= STEP;
                }
                
                if (isEmptyLine) {
                    break;
                }
                
                // Перелистываем график
                robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
                Thread.sleep(100);
                robot.mouseMove(rightWork, xCenter);
                robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
                Thread.sleep(100);
                
                i = rightWork;
            }    
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }
    }
    
    public Set<Candle> getCandles(Period p) {
        switch (p) {
            case DAY: return candles1D;
            case HOUR: return candles1H;
            default: new RuntimeException(p + " doesn't support yet.");
        }
        return null;
    }
    
}
