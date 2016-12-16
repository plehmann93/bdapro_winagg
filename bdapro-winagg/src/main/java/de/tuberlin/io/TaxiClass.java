package de.tuberlin.io;

/**
 * Created by patrick on 16.12.16.
 */
public  class TaxiClass {

    public int getPassengerCnt(String line){
        String[] values=line.split(",");

        int passengerCnt=Integer.valueOf(values[8]);
        return passengerCnt;
    }
}
