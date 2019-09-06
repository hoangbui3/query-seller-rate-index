package vn.tiki.discovery.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CommonUtils
{
	public static String readAll(InputStream is) throws IOException {
		StringBuilder result = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			String line;
			while ((line = reader.readLine()) != null) {
				result.append(line).append("\n");
			}
		}
		return result.toString();
	}
}
