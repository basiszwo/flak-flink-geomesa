// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.

package one.flak.accumuloingest;

import com.csvreader.CsvReader;
import one.flak.flinkgeomesa.models.TripSample;
import one.flak.flinkgeomesa.models.TripSampleBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TripSampleCsvReader {

    private CsvReader reader;

    public TripSampleCsvReader(String filePath) throws IOException {
        this.reader = new CsvReader(filePath, ',');

        reader.readHeaders();
    }

    public TripSample next() throws IOException {
        // builder
        TripSampleBuilder builder = new TripSampleBuilder();

        builder.setOccuredAt(SafeCsvGetter.getLong(reader.get(0)));
        builder.setAccelerationZ(SafeCsvGetter.getDouble(reader.get(8)));
        builder.setLatitude(SafeCsvGetter.getDouble(reader.get(9)));
        builder.setLongitude(SafeCsvGetter.getDouble(reader.get(10)));

        return builder.createTripSample();
    }


    public boolean hasNext() {
        try {
            return this.reader.readRecord();
        } catch (IOException e) {
            e.printStackTrace();
            return true;
        }
    }

}
