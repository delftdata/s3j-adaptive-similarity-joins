package Utils;

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.java.tuple.Tuple2;


public class ParetoTopic2D {

    private ParetoDistribution paretoX;
    private ParetoDistribution paretoY;
    private final int signX;
    private final int signY;
    private final double offsetX;
    private final double offsetY;

    public ParetoTopic2D(double scaleX, double scaleY, double shapeX, double shapeY, int signX, int signY){
        this.paretoX = new ParetoDistribution(new Well19937c(70), scaleX, shapeX);
        this.paretoY = new ParetoDistribution(new Well19937c(42), scaleY, shapeY);
        this.signX = signX;
        this.signY = signY;
        this.offsetX = 0;
        this.offsetY = 0;
    }

    public ParetoTopic2D(double scaleX, double scaleY, double shapeX, double shapeY, int signX, int signY, double offsetX, double offsetY){
        this.paretoX = new ParetoDistribution(new Well19937c(70), scaleX, shapeX);
        this.paretoY = new ParetoDistribution(new Well19937c(42), scaleY, shapeY);
        this.signX = signX;
        this.signY = signY;
        this.offsetX = offsetX;
        this.offsetY = offsetY;
    }

    public Tuple2<Double, Double> getParetoTuple(){
        double corX = paretoX.sample();
        double corY = paretoY.sample();
        return new Tuple2<>(corX * this.signX - this.offsetX * this.signX, corY * this.signY - this.offsetY * this.signY);
    }

    public ParetoDistribution getParetoX() {
        return paretoX;
    }

    public void setParetoX(ParetoDistribution paretoX) {
        this.paretoX = paretoX;
    }

    public ParetoDistribution getParetoY() {
        return paretoY;
    }

    public void setParetoY(ParetoDistribution paretoY) {
        this.paretoY = paretoY;
    }
}
