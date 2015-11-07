package fire.datacleanup;

/**
 * Created by jayantshekhar
 */
public class RemoveNonPrintableCharacters {
    public static void main(String[] args)
    {

        String string = "aaabbb\t\nccc";
        string = removeNonPrintableCharacters(string);
        System.out.println(string);
    }

    // http://stackoverflow.com/questions/6198986/how-can-i-replace-non-printable-unicode-characters-in-java
    public static String removeNonPrintableCharacters(String string)
    {
        return string.replaceAll("\\p{C}", "");
    }
}
