/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eva.graber;

/**
 *
 * @author Natali
 */
public class Candle {
    // Start time
    private long time;
    
    // Значения экстремумов сети
    private double hight;
    private double low;
    
    // Значения входа и выхода свечи
    private double in;
    private double out;
    
    // Объемы закрытых лотов в свече на покупки и продажи
    private double volume;
    
    public Candle (long t, double h, double l, double i, double o, double v) {
        this.time = t;
        this.hight = h;
        this.low = l;
        this.in = i;
        this.out = o;
        this.volume = v;
    }

    public long getTime() {
        return time;
    }

    public double getHight() {
        return hight;
    }

    public double getLow() {
        return low;
    }

    public double getIn() {
        return in;
    }

    public double getOut() {
        return out;
    }

    public double getVolume() {
        return volume;
    }
    
    @Override
    public boolean equals (Object o) {
        return ((Candle)o).time == this.time;
    }
    
    
}
