package one.flak.flinkgeomesa.models;

import java.util.Date;
import java.util.UUID;

public class TripSampleBuilder {
    private Date occuredAt;
    private String tripIdentifier = "";
    private double latitude;
    private double longitude;
    private String vin = "";
    private double accelerationX = 0.0;
    private double accelerationY = 0.0;
    private double accelerationZ;

    public TripSampleBuilder setOccuredAt(Date occuredAt) {
        this.occuredAt = occuredAt;
        return this;
    }

    public TripSampleBuilder setOccuredAt(long occuredAt) {
        this.occuredAt = new Date(occuredAt);
        return this;
    }

    public TripSampleBuilder setTripIdentifier(String tripIdentifier) {
        // this.tripIdentifier = tripIdentifier;
        this.tripIdentifier = String.valueOf(UUID.randomUUID());
        return this;
    }

    public TripSampleBuilder setLatitude(double latitude) {
        this.latitude = latitude;
        return this;
    }

    public TripSampleBuilder setLongitude(double longitude) {
        this.longitude = longitude;
        return this;
    }

    public TripSampleBuilder setVin(String vin) {
        this.vin = vin;
        return this;
    }

    public TripSampleBuilder setAccelerationX(double accelerationX) {
        this.accelerationX = accelerationX;
        return this;
    }

    public TripSampleBuilder setAccelerationY(double accelerationY) {
        this.accelerationY = accelerationY;
        return this;
    }

    public TripSampleBuilder setAccelerationZ(double accelerationZ) {
        this.accelerationZ = accelerationZ;
        return this;
    }

    public TripSample createTripSample() {
        return new TripSample(occuredAt, tripIdentifier, latitude, longitude, vin, accelerationX, accelerationY, accelerationZ);
    }
}