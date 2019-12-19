public class Utils {

    public static String getSeparateur(String line) {
        char[] separateurs = { '|', '\t', ';', '/', ',' };
        for (int i = 0; i < line.length(); i++) {
            for(char separateur: separateurs) {
                if(line.charAt(i) == separateur)
                    return "" + separateur;
            }
        }
        return "";
    }
}
