package com.gyf.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UserAgent测试类
 */
public class UserAgentTest
{
    @Test
    public void testReadFile() throws Exception
    {
        String path = "/Users/gaoyunfan/Desktop/10000_access.log";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
        String line = "";
        int i=0;
        UserAgentParser userAgentParser;
        UserAgent agent;
        Map<String, Integer> browserMap = new HashMap<String, Integer>();
        while (line != null)
        {

            line = reader.readLine();
            if (StringUtils.isNotBlank(line))
            {
                String source = line.substring(getCharacterPosition(line, "\"", 7) + 1, getCharacterPosition(line, "\"", 8));
                userAgentParser = new UserAgentParser();
                agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                boolean mobile = agent.isMobile();
                if (browserMap.get(browser) != null)
                {
                    browserMap.put(browser, browserMap.get(browser) + 1);
                }else {
                    browserMap.put(browser, 1);
                }
//                System.out.println(browser + " , " + engine + "  , " + engineVersion + " , " + os + " , " + platform + " , " + mobile + " , ");
                i++;
            }

        }
        System.out.println(i);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        for (Map.Entry<String, Integer> entry : browserMap.entrySet())
        {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Test
    public void testGetCharacterPosition()
    {
        String value = "171.43.210.54 - - [10/Nov/2016:00:01:53 +0800] \"POST /course/ajaxmediauser HTTP/1.1\" 200 54 \"www.imooc.com\" \"http://www.imooc.com/code/2276\" mid=2276&time=60 \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36\" \"-\" 10.100.136.65:80 200 0.015 0.015";
        int fromIndex = getCharacterPosition(value, "\"", 7);
        int toIndex = getCharacterPosition(value, "\"", 8);
        System.out.println(value.substring(fromIndex + 1, toIndex));
    }

    /**
     * 获取指定字符串中指定表示的字符串出现的索引位置
     *
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private int getCharacterPosition(String value, String operator, int index)
    {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (slashMatcher.find())
        {
            mIdx++;
            if (mIdx == index)
            {
                break;
            }
        }
        return slashMatcher.start();
    }

    /**
     * 单元测试：UserAgent工具类的使用
     */
    @Test
    public void testUserAgentParser()
    {
//        String source = "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.16.2.3 Basic ECC zlib/1.2.3 libidn/1.18 libssh2/1.4.2";
        String source = "mukewang/5.0.0 (Android 5.0.2; motorola Moto X Pro Build/LXG22.67-7.1),Network WIFI";
        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean mobile = agent.isMobile();
        System.out.println(browser + " , " + engine + "  , " + engineVersion + " , " + os + " , " + platform + " , " + mobile + " , ");
    }
}
