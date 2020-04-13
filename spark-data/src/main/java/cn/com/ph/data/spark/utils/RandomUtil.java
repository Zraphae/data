package cn.com.ph.data.spark.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class RandomUtil {

    private final static Random random = new Random();

    public static String makeRowKey(String rowKey){
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.reset();
            messageDigest.update(rowKey.getBytes());
            byte[] bytes = messageDigest.digest();
            String md5Content = new String(Hex.encodeHex(bytes));
            if(!StringUtils.isEmpty(md5Content)){
                String substring = md5Content.substring(0, 8);
                while (substring.length() < 8){
                    substring = "0" + substring;
                }
                return substring + "_" + rowKey;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return random.nextInt(8) + "_" + rowKey;
    }
}
