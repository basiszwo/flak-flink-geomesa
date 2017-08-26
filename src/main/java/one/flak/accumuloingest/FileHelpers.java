// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.
package one.flak.accumuloingest;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by sb on 16.05.17.
 */
public class FileHelpers {

    public static JsonElement readJsonFromFile(String filePath) {
        String jsonString;
        try {
            jsonString = new String(Files.readAllBytes(Paths.get(filePath)), Charset.forName("UTF-8"));
        } catch(IOException e) {
            jsonString = "";
        }

        if(jsonString.isEmpty()) {
            return null;
        } else {
            return new JsonParser().parse(jsonString);
        }
    }

}
