// Copyright (c) Stefan Botzenhart. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
package one.flak.flinkgeomesa.models;

import java.util.Date;

public class TripSample {

    private Date occuredAt;
    private String tripIdentifier;
    private double latitude;
    private double longitude;
    private String vin;
    private double accelerationX;
    private double accelerationY;
    private double accelerationZ;


    public TripSample(Date occuredAt, String tripIdentifier, double latitude, double longitude, String vin, double accelerationX, double accelerationY, double accelerationZ) {
        this.occuredAt = occuredAt;
        this.tripIdentifier = tripIdentifier;
        this.latitude = latitude;
        this.longitude = longitude;
        this.vin = vin;
        this.accelerationX = accelerationX;
        this.accelerationY = accelerationY;
        this.accelerationZ = accelerationZ;
    }

    public Date getOccuredAt() {
        return occuredAt;
    }

    public String getTripIdentifier() {
        return tripIdentifier;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getVin() {
        return vin;
    }

    public double getAccelerationX() {
        return accelerationX;
    }

    public double getAccelerationY() {
        return accelerationY;
    }

    public double getAccelerationZ() {
        return accelerationZ;
    }
}
