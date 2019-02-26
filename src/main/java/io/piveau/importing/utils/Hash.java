package io.piveau.importing.utils;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;

public class Hash {

    public static String asHexString(String data) {
        byte[] hash = asByteArray(data);
        return hash != null ? DatatypeConverter.printHexBinary(hash) : "";
    }

    public static byte[] asByteArray(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            return digest.digest(data.getBytes("UTF-8"));
        } catch (Exception e) {
            return new byte[]{};
        }
    }

}
