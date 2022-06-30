package cn.northpark.javaSpark.lagouAPP;

import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String eng_String = sc.next();
        if ("".equalsIgnoreCase(eng_String.trim())){
            System.out.println("");
            return;
        }
        String[] split = eng_String.split("(?<=(.))(?!\\1)");

        String result = "";
        String result2 = "";
        for (String s : split) {
            result2+=s+"-";
            result+=s.substring(0,1)+s.length();
        }
        System.out.println("result2:"+result2);
        System.out.println(result);
    }
}
