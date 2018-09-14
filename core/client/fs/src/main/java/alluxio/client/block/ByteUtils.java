package alluxio.client.block;

import java.nio.charset.StandardCharsets;
/**
 * 用于处理各类型与Byte类型的关系
 * 包括转换，与计算大小
 *
 * @author Myth
 */
public class ByteUtils {
    public static byte[] toBytes(int i) {
        return new byte[]{
                (byte) (i >> 24),
                (byte) (i >> 16),
                (byte) (i >> 8),
                (byte) (i)
        };
    }

    public static byte[] toBytes(short i) {
        return new byte[]{
                (byte) (i >> 8),
                (byte) (i)
        };
    }

    public static byte[] toBytes(int i, int size) {
        byte[] bytes = new byte[size];
        for (int j = size; j > 0; j--) {
            bytes[j - 1] = (byte) (i >> (Byte.SIZE * (size - j)));
        }
        return bytes;
    }

    public static byte[] toBytes(float f) {
        int i = Float.floatToIntBits(f);
        return toBytes(i);
    }

    public static byte[] toBytes(long l) {
        return new byte[]{
                (byte) (l >> 56 & 0xff),
                (byte) (l >> 48 & 0xff),
                (byte) (l >> 40 & 0xff),
                (byte) (l >> 32 & 0xff),
                (byte) (l >> 24 & 0xff),
                (byte) (l >> 16 & 0xff),
                (byte) (l >> 8 & 0xff),
                (byte) (l & 0xff)
        };
    }

    public static byte[] toBytes(double d) {
        long l = Double.doubleToLongBits(d);
        return toBytes(l);
    }

    public static byte[] toBytes(String s) {
        if (s == null) {
            return new byte[]{};
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static boolean toBool(int b) {
        return b == 1;
    }

    public static int toShort(byte[] b) {
        int result = 0;
        for (byte aB : b) {
            result <<= Byte.SIZE;
            result |= (aB & 0xff);
        }
        return result;
    }

    public static int toInt(byte[] b) {
        int result = 0;
        for (byte aB : b) {
            result <<= Byte.SIZE;
            result |= (aB & 0xff);
        }
        return result;
    }

    public static long toLong(byte[] b) {
        long result = 0;
        for (byte aB : b) {
            result <<= Byte.SIZE;
            result |= (aB & 0xff);
        }
        return result;
    }

    public static float toFloat(byte[] b) {
        int i = toInt(b);
        return Float.intBitsToFloat(i);
    }

    public static double toDouble(byte[] b) {
        long l = toLong(b);
        return Double.longBitsToDouble(l);
    }

    public static String toString(byte[] b) {
        if (b.length == 0) {
            return null;
        }
        return new String(b, StandardCharsets.UTF_8);
    }
}
