package utils;

import java.sql.Blob;
import java.sql.SQLException;

public class QueryHelper {

    public static String blobToPostgresHex(Blob blobData) throws SQLException {
        char[] hexArray = "0123456789ABCDEF".toCharArray();
        String returnData = "";

        if (blobData != null) {
            try {
                byte[] bytes = blobData.getBytes(1, (int) blobData.length());

                char[] hexChars = new char[bytes.length * 2];
                for (int j = 0; j < bytes.length; j++) {
                    int v = bytes[j] & 0xFF;
                    hexChars[j * 2] = hexArray[v >>> 4];
                    hexChars[j * 2 + 1] = hexArray[v & 0x0F];
                }
                returnData = "\\\\x" + new String(hexChars);
            } finally {
                // The most important thing here is free the BLOB to avoid memory Leaks
                blobData.free();
            }
        }
        return returnData;
    }
}
