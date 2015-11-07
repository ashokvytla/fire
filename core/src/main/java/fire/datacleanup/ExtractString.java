package fire.datacleanup;

/**
 * Created by jayantshekhar
 */
public class ExtractString {

    public static void main(String[] args)
    {

        String string = "aaabbbcccc";

        String result = extractBetweenIndexes(string, 2, 5);
        System.out.println(result);

        result = afterStringBeforeString(string, "b", "c");
        System.out.println(result);

        result = afterString(string, "b");
        System.out.println(result);
    }


    public static String extractBetweenIndexes(String string, int idx1, int idx2)
    {
        String str = string.substring(idx1, idx2);
        return str;
    }

    public static String afterString(String string, String aft)
    {
        int idx1 = string.indexOf(aft);
        idx1 += aft.length();

        return string.substring(idx1);
    }

    public static String afterStringBeforeString(String string, String aft, String b4)
    {
        int idx1 = string.indexOf(aft);
        idx1 += aft.length();

        int idx2 = string.indexOf(b4);

        return string.substring(idx1, idx2);
    }

}
