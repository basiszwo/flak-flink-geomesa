// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.
package one.flak.flinkgeomesa.models;

import java.util.Date;

public class TripSample {

    private Date occuredAt;
    private String tripIdentifier;
    private double latitude;
    private double longitude;
    private double accelerationX;
    private double accelerationY;
    private double accelerationZ;


    public TripSample(Date occuredAt, String tripIdentifier, double latitude, double longitude, double accelerationX, double accelerationY, double accelerationZ) {
        this.occuredAt = occuredAt;
        this.tripIdentifier = tripIdentifier;
        this.latitude = latitude;
        this.longitude = longitude;
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
