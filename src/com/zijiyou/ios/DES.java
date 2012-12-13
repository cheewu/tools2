package com.zijiyou.ios;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class DES {
 
   public static final String PASSWORD_CRYPT_KEY="ZJYHK520";
     
   /**
    * 密码加密
    * 
    * @param password
    * @return
    * @throws Exception
    */
   public final static String encrypt(String password) {
       try {
           return byte2hex(encrypt(password.getBytes("UTF8"),
                   PASSWORD_CRYPT_KEY.getBytes("UTF8")));
       } catch (Exception e) {
       }
       return null;
   }
   
   /**
    * 加密
    * @param src
    *            数据源
    * @param key
    *            密钥，长度必须是8的倍数
    * @return 返回加密后的数据
    * @throws Exception
    */
   public static byte[] encrypt(byte[] src, byte[] key) throws Exception {
	   
       // DES算法要求有一个可信任的随机数源
       SecureRandom sr = new SecureRandom();
       // 从原始密匙数据创建DESKeySpec对象
       DESKeySpec dks = new DESKeySpec(key);
       // 创建一个密匙工厂，然后用它把DESKeySpec转换成
       // 一个SecretKey对象
       SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
       SecretKey securekey = keyFactory.generateSecret(dks);
       // Cipher对象实际完成加密操作
       Cipher cipher = Cipher.getInstance("DES");
       // 用密匙初始化Cipher对象
       cipher.init(Cipher.ENCRYPT_MODE, securekey, sr);
       // 现在，获取数据并加密
       // 正式执行加密操作
       return cipher.doFinal(src);
   }
   
     
     public  static void main(String args[]) throws Exception{
//    	 byte[] encryImage=encrypt(SQLite.readImage("select.xml"),PASSWORD_CRYPT_KEY.getBytes("UTF8"));
//    	 System.out.println(byte2hex(encryImage)+"\n"+encryImage.length);
    	 
    	 System.out.println(encrypt("我们在北京"));
     }
     
     /**
      * 二行制转字符串
      * 
      * @param b
      * @return
      */
     public static String byte2hex(byte[] b) {
         String hs = "";
         String stmp = "";
         for (int n = 0; n < b.length; n++) {
             stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
             if (stmp.length() == 1)
                 hs = hs + "0" + stmp;
             else
                 hs = hs + stmp;
         }
         return hs.toUpperCase();
     }
     
 }